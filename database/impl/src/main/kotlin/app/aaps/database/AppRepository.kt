package app.aaps.database

import androidx.room.withTransaction
import app.aaps.database.entities.APSResult
import app.aaps.database.entities.Bolus
import app.aaps.database.entities.BolusCalculatorResult
import app.aaps.database.entities.Carbs
import app.aaps.database.entities.DeviceStatus
import app.aaps.database.entities.EffectiveProfileSwitch
import app.aaps.database.entities.ExtendedBolus
import app.aaps.database.entities.Food
import app.aaps.database.entities.GlucoseValue
import app.aaps.database.entities.HeartRate
import app.aaps.database.entities.ProfileSwitch
import app.aaps.database.entities.RunningMode
import app.aaps.database.entities.StepsCount
import app.aaps.database.entities.TemporaryBasal
import app.aaps.database.entities.TemporaryTarget
import app.aaps.database.entities.TherapyEvent
import app.aaps.database.entities.TotalDailyDose
import app.aaps.database.entities.UserEntry
import app.aaps.database.entities.data.NewEntries
import app.aaps.database.entities.embedments.InterfaceIDs
import app.aaps.database.entities.interfaces.DBEntry
import app.aaps.database.transactions.Transaction
import io.reactivex.rxjava3.core.Completable
import io.reactivex.rxjava3.core.Maybe
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.schedulers.Schedulers
import io.reactivex.rxjava3.subjects.PublishSubject
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.rx3.rxMaybe
import kotlinx.coroutines.rx3.rxSingle
import java.io.Closeable
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit
import javax.inject.Inject
import javax.inject.Singleton
import kotlin.math.roundToInt

@Singleton
class AppRepository @Inject internal constructor(
    internal val database: AppDatabase
) : Closeable {

    // ========== RXJAVA SUPPORT (EXISTING) ==========
    private val changeSubject = PublishSubject.create<List<DBEntry>>()

    fun changeObservable(): Observable<List<DBEntry>> = changeSubject.subscribeOn(Schedulers.io())

    // ========== FLOW SUPPORT (NEW) ==========

    /**
     * Coroutine scope for Flow emissions
     * Using SupervisorJob so failures don't cancel the entire scope
     */
    private val repositoryScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    /**
     * SharedFlow for broadcasting database changes
     * - replay = 0: No replay, only new changes
     * - extraBufferCapacity = 64: Buffer fast emissions
     * - onBufferOverflow = DROP_OLDEST: Drop old events if buffer full
     */
    private val _changeFlow = MutableSharedFlow<List<DBEntry>>(
        replay = 0,
        extraBufferCapacity = 64,
        onBufferOverflow = kotlinx.coroutines.channels.BufferOverflow.DROP_OLDEST
    )

    /**
     * Observe ALL database changes as Flow
     */
    fun changeFlow(): Flow<List<DBEntry>> = _changeFlow.asSharedFlow()

    /**
     * Observe database changes filtered by entity type
     * Example: repository.changesOfType<TemporaryBasal>()
     */
    inline fun <reified T : DBEntry> changesOfType(): Flow<List<T>> =
        changeFlow()
            .map { changes -> changes.filterIsInstance<T>() }
            .filter { it.isNotEmpty() }

    /**
     * Executes a transaction ignoring its result (coroutine version)
     * Uses Room's suspend withTransaction API for proper coroutine support
     * Emits to BOTH RxJava (existing) AND Flow (new)
     */
    suspend fun <T> runTransactionSuspend(transaction: Transaction<T>) {
        val changes = mutableListOf<DBEntry>()
        database.withTransaction {
            transaction.database = DelegatedAppDatabase(changes, database)
            transaction.run()
        }
        // Emit to RxJava (existing) - for backwards compatibility
        changeSubject.onNext(changes)

        // Emit to Flow (new)
        if (changes.isNotEmpty()) {
            _changeFlow.emit(changes)
        }
    }

    /**
     * Executes a transaction and returns its result (coroutine version)
     * Uses Room's suspend withTransaction API for proper coroutine support
     * Emits to BOTH RxJava (existing) AND Flow (new)
     */
    suspend fun <T : Any> runTransactionForResultSuspend(transaction: Transaction<T>): T {
        val changes = mutableListOf<DBEntry>()
        val result = database.withTransaction {
            transaction.database = DelegatedAppDatabase(changes, database)
            transaction.run()
        }
        // Emit to RxJava (existing) - for backwards compatibility
        changeSubject.onNext(changes)

        // Emit to Flow (new)
        if (changes.isNotEmpty()) {
            _changeFlow.emit(changes)
        }
        return result
    }

    /**
     * Executes a transaction ignoring its result (RxJava version)
     * Runs on IO scheduler
     * Emits to BOTH RxJava (existing) AND Flow (new)
     */
    fun <T> runTransaction(transaction: Transaction<T>): Completable {
        val changes = mutableListOf<DBEntry>()
        return Completable.fromCallable {
            database.runInTransaction {
                transaction.database = DelegatedAppDatabase(changes, database)
                runBlocking { transaction.run() }
            }
        }.subscribeOn(Schedulers.io()).doOnComplete {
            // Emit to RxJava (existing) - for backwards compatibility
            changeSubject.onNext(changes)

            // Emit to Flow (new)
            if (changes.isNotEmpty()) {
                repositoryScope.launch {
                    _changeFlow.emit(changes)
                }
            }
        }
    }

    /**
     * Executes a transaction and returns its result (RxJava version)
     * Runs on IO scheduler
     * Emits to BOTH RxJava (existing) AND Flow (new)
     */
    fun <T : Any> runTransactionForResult(transaction: Transaction<T>): Single<T> {
        val changes = mutableListOf<DBEntry>()
        return Single.fromCallable {
            database.runInTransaction(Callable {
                transaction.database = DelegatedAppDatabase(changes, database)
                runBlocking { transaction.run() }
            })
        }.subscribeOn(Schedulers.io()).doOnSuccess {
            // Emit to RxJava (existing) - for backwards compatibility
            changeSubject.onNext(changes)

            // Emit to Flow (new)
            if (changes.isNotEmpty()) {
                repositoryScope.launch {
                    _changeFlow.emit(changes)
                }
            }
        }
    }

    fun clearDatabases() = database.clearAllTables()

    fun clearApsResults() = database.apsResultDao.deleteAllEntries()

    suspend fun cleanupDatabase(keepDays: Long, deleteTrackedChanges: Boolean): String {
        val than = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(keepDays)
        val removed = mutableListOf<Pair<String, Int>>()
        removed.add(Pair("APSResult", database.apsResultDao.deleteOlderThan(than)))
        removed.add(Pair("GlucoseValue", database.glucoseValueDao.deleteOlderThan(than)))
        removed.add(Pair("TherapyEvent", database.therapyEventDao.deleteOlderThan(than)))
        removed.add(Pair("TemporaryBasal", database.temporaryBasalDao.deleteOlderThan(than)))
        removed.add(Pair("ExtendedBolus", database.extendedBolusDao.deleteOlderThan(than)))
        removed.add(Pair("Bolus", database.bolusDao.deleteOlderThan(than)))
        removed.add(Pair("TotalDailyDose", database.totalDailyDoseDao.deleteOlderThan(than)))
        removed.add(Pair("Carbs", database.carbsDao.deleteOlderThan(than)))
        removed.add(Pair("TemporaryTarget", database.temporaryTargetDao.deleteOlderThan(than)))
        removed.add(Pair("BolusCalculatorResult", database.bolusCalculatorResultDao.deleteOlderThan(than)))
        // keep at least one EPS
        if (database.effectiveProfileSwitchDao.getEffectiveProfileSwitchDataFromTime(than + 1).isNotEmpty())
            removed.add(Pair("EffectiveProfileSwitch", database.effectiveProfileSwitchDao.deleteOlderThan(than)))
        removed.add(Pair("ProfileSwitch", database.profileSwitchDao.deleteOlderThan(than)))
        removed.add(Pair("ApsResult", database.apsResultDao.deleteOlderThan(than)))
        // keep version history database.versionChangeDao.deleteOlderThan(than)
        removed.add(Pair("UserEntry", database.userEntryDao.deleteOlderThan(than)))
        removed.add(Pair("PreferenceChange", database.preferenceChangeDao.deleteOlderThan(than)))
        // keep foods database.foodDao.deleteOlderThan(than)
        removed.add(Pair("DeviceStatus", database.deviceStatusDao.deleteOlderThan(than)))
        removed.add(Pair("RunningMode", database.runningModeDao.deleteOlderThan(than)))
        removed.add(Pair("HeartRate", database.heartRateDao.deleteOlderThan(than)))
        removed.add(Pair("StepsCount", database.stepsCountDao.deleteOlderThan(than)))

        if (deleteTrackedChanges) {
            removed.add(Pair("CHANGES APSResult", database.apsResultDao.deleteTrackedChanges()))
            removed.add(Pair("CHANGES GlucoseValue", database.glucoseValueDao.deleteTrackedChanges()))
            removed.add(Pair("CHANGES TherapyEvent", database.therapyEventDao.deleteTrackedChanges()))
            removed.add(Pair("CHANGES TemporaryBasal", database.temporaryBasalDao.deleteTrackedChanges()))
            removed.add(Pair("CHANGES Bolus", database.bolusDao.deleteTrackedChanges()))
            removed.add(Pair("CHANGES ExtendedBolus", database.extendedBolusDao.deleteTrackedChanges()))
            removed.add(Pair("CHANGES TotalDailyDose", database.totalDailyDoseDao.deleteTrackedChanges()))
            removed.add(Pair("CHANGES Carbs", database.carbsDao.deleteTrackedChanges()))
            removed.add(Pair("CHANGES TemporaryTarget", database.temporaryTargetDao.deleteTrackedChanges()))
            removed.add(Pair("CHANGES BolusCalculatorResult", database.bolusCalculatorResultDao.deleteTrackedChanges()))
            removed.add(Pair("CHANGES EffectiveProfileSwitch", database.effectiveProfileSwitchDao.deleteTrackedChanges()))
            removed.add(Pair("CHANGES ProfileSwitch", database.profileSwitchDao.deleteTrackedChanges()))
            removed.add(Pair("CHANGES ApsResult", database.apsResultDao.deleteTrackedChanges()))
            // keep food database.foodDao.deleteHistory()
            removed.add(Pair("CHANGES RunningMode", database.runningModeDao.deleteTrackedChanges()))
            removed.add(Pair("CHANGES HeartRate", database.heartRateDao.deleteTrackedChanges()))
            removed.add(Pair("CHANGES StepsCount", database.stepsCountDao.deleteTrackedChanges()))
        }
        val ret = StringBuilder()
        removed
            .filter { it.second > 0 }
            .map { ret.append(it.first + " " + it.second + "<br>") }
        return ret.toString()
    }

    suspend fun clearCachedTddData(from: Long) = database.totalDailyDoseDao.deleteNewerThan(from, InterfaceIDs.PumpType.CACHE)

    //BG READINGS -- only valid records
    suspend fun compatGetBgReadingsDataFromTime(timestamp: Long, ascending: Boolean): List<GlucoseValue> =
        database.glucoseValueDao.compatGetBgReadingsDataFromTime(timestamp).reversedIf(!ascending)

    suspend fun compatGetBgReadingsDataFromTime(start: Long, end: Long, ascending: Boolean): List<GlucoseValue> =
        database.glucoseValueDao.compatGetBgReadingsDataFromTime(start, end).reversedIf(!ascending)

    //BG READINGS -- including invalid/history records
    suspend fun findBgReadingByNSId(nsId: String): GlucoseValue? =
        database.glucoseValueDao.findByNSId(nsId)

    suspend fun getLastGlucoseValueId(): Long? =
        database.glucoseValueDao.getLastId()

    suspend fun getLastGlucoseValue(): GlucoseValue? =
        database.glucoseValueDao.getLast()

    /*
       * returns a Pair of the next entity to sync and the ID of the "update".
       * The update id might either be the entry id itself if it is a new entry - or the id
       * of the update ("historic") entry. The sync counter should be incremented to that id if it was synced successfully.
       *
       * It is a Maybe as there might be no next element.
       * */
    fun getNextSyncElementGlucoseValue(id: Long): Maybe<Pair<GlucoseValue, GlucoseValue>> = rxMaybe {
        val nextIdElement = database.glucoseValueDao.getNextModifiedOrNewAfter(id) ?: return@rxMaybe null
        val nextIdElemReferenceId = nextIdElement.referenceId
        if (nextIdElemReferenceId == null) {
            nextIdElement to nextIdElement
        } else {
            val historic = database.glucoseValueDao.getCurrentFromHistoric(nextIdElemReferenceId) ?: return@rxMaybe null
            historic to nextIdElement
        }
    }

    // TEMP TARGETS
    suspend fun findTemporaryTargetByNSId(nsId: String): TemporaryTarget? =
        database.temporaryTargetDao.findByNSId(nsId)

    /*
       * returns a Pair of the next entity to sync and the ID of the "update".
       * The update id might either be the entry id itself if it is a new entry - or the id
       * of the update ("historic") entry. The sync counter should be incremented to that id if it was synced successfully.
       *
       * It is a Maybe as there might be no next element.
       * */
    fun getNextSyncElementTemporaryTarget(id: Long): Maybe<Pair<TemporaryTarget, TemporaryTarget>> = rxMaybe {
        val nextIdElement = database.temporaryTargetDao.getNextModifiedOrNewAfter(id) ?: return@rxMaybe null
        val nextIdElemReferenceId = nextIdElement.referenceId
        if (nextIdElemReferenceId == null) {
            nextIdElement to nextIdElement
        } else {
            val historic = database.temporaryTargetDao.getCurrentFromHistoric(nextIdElemReferenceId)
            historic?.let { it to nextIdElement }
        }
    }

    suspend fun getTemporaryTargetDataFromTime(timestamp: Long, ascending: Boolean): List<TemporaryTarget> =
        database.temporaryTargetDao.getTemporaryTargetDataFromTime(timestamp).reversedIf(!ascending)

    suspend fun getTemporaryTargetDataIncludingInvalidFromTime(timestamp: Long, ascending: Boolean): List<TemporaryTarget> =
        database.temporaryTargetDao.getTemporaryTargetDataIncludingInvalidFromTime(timestamp).reversedIf(!ascending)

    suspend fun getTemporaryTargetActiveAt(timestamp: Long): TemporaryTarget? =
        database.temporaryTargetDao.getTemporaryTargetActiveAt(timestamp)

    suspend fun getLastTempTargetId(): Long? =
        database.temporaryTargetDao.getLastId()

    // USER ENTRY
    suspend fun getUserEntryDataFromTime(timestamp: Long): List<UserEntry> =
        database.userEntryDao.getUserEntryDataFromTime(timestamp)

    suspend fun getUserEntryFilteredDataFromTime(timestamp: Long): List<UserEntry> =
        database.userEntryDao.getUserEntryFilteredDataFromTime(UserEntry.Sources.Loop, timestamp)

    suspend fun insert(word: UserEntry) {
        database.userEntryDao.insert(word)
        changeSubject.onNext(mutableListOf(word)) // Not TraceableDao
    }

    // PROFILE SWITCH

    suspend fun findProfileSwitchByNSId(nsId: String): ProfileSwitch? =
        database.profileSwitchDao.findByNSId(nsId)

    fun getNextSyncElementProfileSwitch(id: Long): Maybe<Pair<ProfileSwitch, ProfileSwitch>> = rxMaybe {
        val nextIdElement = database.profileSwitchDao.getNextModifiedOrNewAfter(id) ?: return@rxMaybe null
        val nextIdElemReferenceId = nextIdElement.referenceId
        if (nextIdElemReferenceId == null) {
            nextIdElement to nextIdElement
        } else {
            val historic = database.profileSwitchDao.getCurrentFromHistoric(nextIdElemReferenceId)
            historic?.let { it to nextIdElement }
        }
    }

    suspend fun getProfileSwitchActiveAt(timestamp: Long): ProfileSwitch? {
        val tps = database.profileSwitchDao.getTemporaryProfileSwitchActiveAt(timestamp)
        val ps = database.profileSwitchDao.getPermanentProfileSwitchActiveAt(timestamp)
        if (tps != null && ps != null)
            return if (ps.timestamp > tps.timestamp) ps else tps
        if (ps == null) return tps
        return ps // if (tps == null)
    }

    fun getPermanentProfileSwitchActiveAt(timestamp: Long): Maybe<ProfileSwitch> = rxMaybe {
        database.profileSwitchDao.getPermanentProfileSwitchActiveAt(timestamp)
    }

    fun getAllProfileSwitches(): Single<List<ProfileSwitch>> = rxSingle {
        database.profileSwitchDao.getAllProfileSwitches()
    }

    suspend fun getProfileSwitchesFromTime(timestamp: Long, ascending: Boolean): List<ProfileSwitch> =
        database.profileSwitchDao.getProfileSwitchDataFromTime(timestamp).reversedIf(!ascending)

    suspend fun getProfileSwitchesIncludingInvalidFromTime(timestamp: Long, ascending: Boolean): List<ProfileSwitch> =
        database.profileSwitchDao.getProfileSwitchDataIncludingInvalidFromTime(timestamp).reversedIf(!ascending)

    suspend fun getLastProfileSwitchId(): Long? =
        database.profileSwitchDao.getLastId()

    // RUNNING MODE

    suspend fun findRunningModeByNSId(nsId: String): RunningMode? =
        database.runningModeDao.findByNSId(nsId)

    fun getNextSyncElementRunningMode(id: Long): Maybe<Pair<RunningMode, RunningMode>> = rxMaybe {
        val nextIdElement = database.runningModeDao.getNextModifiedOrNewAfter(id) ?: return@rxMaybe null
        val nextIdElemReferenceId = nextIdElement.referenceId
        if (nextIdElemReferenceId == null) {
            nextIdElement to nextIdElement
        } else {
            val historic = database.runningModeDao.getCurrentFromHistoric(nextIdElemReferenceId)
            historic?.let { it to nextIdElement }
        }
    }

    suspend fun getRunningModeActiveAt(timestamp: Long): RunningMode? {
        val trm = database.runningModeDao.getTemporaryRunningModeActiveAt(timestamp)
        val prm = database.runningModeDao.getPermanentRunningModeActiveAt(timestamp)
        if (trm != null && prm != null)
            return if (prm.timestamp > trm.timestamp) prm else trm
        if (prm == null) return trm
        return prm // if (trm == null)
    }

    fun getPermanentRunningModeActiveAt(timestamp: Long): Maybe<RunningMode> = rxMaybe {
        database.runningModeDao.getPermanentRunningModeActiveAt(timestamp)
    }

    fun getAllRunningModes(): Single<List<RunningMode>> = rxSingle {
        database.runningModeDao.getAllRunningModes()
    }

    suspend fun getRunningModesFromTime(timestamp: Long, ascending: Boolean): List<RunningMode> =
        database.runningModeDao.getRunningModeDataFromTime(timestamp).reversedIf(!ascending)

    fun getRunningModesFromTimeToTime(startTime: Long, endTime: Long, ascending: Boolean): Single<List<RunningMode>> = rxSingle {
        val result = database.runningModeDao.getRunningModeDataFromTimeToTime(startTime, endTime)
        if (!ascending) result.reversed() else result
    }

    suspend fun getRunningModesIncludingInvalidFromTime(timestamp: Long, ascending: Boolean): List<RunningMode> =
        database.runningModeDao.getRunningModeDataIncludingInvalidFromTime(timestamp).reversedIf(!ascending)

    suspend fun getLastRunningModeId(): Long? =
        database.runningModeDao.getLastId()

    // EFFECTIVE PROFILE SWITCH
    suspend fun findEffectiveProfileSwitchByNSId(nsId: String): EffectiveProfileSwitch? =
        database.effectiveProfileSwitchDao.findByNSId(nsId)

    /*
       * returns a Pair of the next entity to sync and the ID of the "update".
       * The update id might either be the entry id itself if it is a new entry - or the id
       * of the update ("historic") entry. The sync counter should be incremented to that id if it was synced successfully.
       *
       * It is a Maybe as there might be no next element.
       * */
    fun getNextSyncElementEffectiveProfileSwitch(id: Long): Maybe<Pair<EffectiveProfileSwitch, EffectiveProfileSwitch>> = rxMaybe {
        val nextIdElement = database.effectiveProfileSwitchDao.getNextModifiedOrNewAfter(id) ?: return@rxMaybe null
        val nextIdElemReferenceId = nextIdElement.referenceId
        if (nextIdElemReferenceId == null) {
            nextIdElement to nextIdElement
        } else {
            val historic = database.effectiveProfileSwitchDao.getCurrentFromHistoric(nextIdElemReferenceId)
            historic?.let { it to nextIdElement }
        }
    }

    fun getOldestEffectiveProfileSwitchRecord(): Maybe<EffectiveProfileSwitch> = rxMaybe {
        database.effectiveProfileSwitchDao.getOldestEffectiveProfileSwitchRecord()
    }

    fun getEffectiveProfileSwitchActiveAt(timestamp: Long): Maybe<EffectiveProfileSwitch> = rxMaybe {
        database.effectiveProfileSwitchDao.getEffectiveProfileSwitchActiveAt(timestamp)
    }

    suspend fun getEffectiveProfileSwitchesFromTime(timestamp: Long, ascending: Boolean): List<EffectiveProfileSwitch> =
        database.effectiveProfileSwitchDao.getEffectiveProfileSwitchDataFromTime(timestamp).reversedIf(!ascending)

    suspend fun getEffectiveProfileSwitchesIncludingInvalidFromTime(timestamp: Long, ascending: Boolean): List<EffectiveProfileSwitch> =
        database.effectiveProfileSwitchDao.getEffectiveProfileSwitchDataIncludingInvalidFromTime(timestamp).reversedIf(!ascending)

    fun getEffectiveProfileSwitchesFromTimeToTime(start: Long, end: Long, ascending: Boolean): Single<List<EffectiveProfileSwitch>> = rxSingle {
        val result = database.effectiveProfileSwitchDao.getEffectiveProfileSwitchDataFromTimeToTime(start, end)
        if (!ascending) result.reversed() else result
    }

    suspend fun getLastEffectiveProfileSwitchId(): Long? =
        database.effectiveProfileSwitchDao.getLastId()

    // THERAPY EVENT
    /*
       * returns a Pair of the next entity to sync and the ID of the "update".
       * The update id might either be the entry id itself if it is a new entry - or the id
       * of the update ("historic") entry. The sync counter should be incremented to that id if it was synced successfully.
       *
       * It is a Maybe as there might be no next element.
       * */
    suspend fun findTherapyEventByNSId(nsId: String): TherapyEvent? =
        database.therapyEventDao.findByNSId(nsId)

    fun getNextSyncElementTherapyEvent(id: Long): Maybe<Pair<TherapyEvent, TherapyEvent>> = rxMaybe {
        val nextIdElement = database.therapyEventDao.getNextModifiedOrNewAfter(id) ?: return@rxMaybe null
        val nextIdElemReferenceId = nextIdElement.referenceId
        if (nextIdElemReferenceId == null) {
            nextIdElement to nextIdElement
        } else {
            val historic = database.therapyEventDao.getCurrentFromHistoric(nextIdElemReferenceId)
            historic?.let { it to nextIdElement }
        }
    }

    suspend fun getTherapyEventDataFromTime(timestamp: Long, ascending: Boolean): List<TherapyEvent> =
        database.therapyEventDao.getTherapyEventDataFromTime(timestamp).reversedIf(!ascending)

    fun getTherapyEventDataFromTime(timestamp: Long, type: TherapyEvent.Type, ascending: Boolean): Single<List<TherapyEvent>> = rxSingle {
        val result = database.therapyEventDao.getTherapyEventDataFromTime(timestamp, type)
        if (!ascending) result.reversed() else result
    }

    suspend fun getTherapyEventDataIncludingInvalidFromTime(timestamp: Long, ascending: Boolean): List<TherapyEvent> =
        database.therapyEventDao.getTherapyEventDataIncludingInvalidFromTime(timestamp).reversedIf(!ascending)

    fun getLastTherapyRecordUpToNow(type: TherapyEvent.Type): Maybe<TherapyEvent> = rxMaybe {
        database.therapyEventDao.getLastTherapyRecord(type, System.currentTimeMillis())
    }

    fun compatGetTherapyEventDataFromToTime(from: Long, to: Long): Single<List<TherapyEvent>> = rxSingle {
        database.therapyEventDao.compatGetTherapyEventDataFromToTime(from, to)
    }

    suspend fun getLastTherapyEventId(): Long? =
        database.therapyEventDao.getLastId()

    // FOOD
    /*
       * returns a Pair of the next entity to sync and the ID of the "update".
       * The update id might either be the entry id itself if it is a new entry - or the id
       * of the update ("historic") entry. The sync counter should be incremented to that id if it was synced successfully.
       *
       * It is a Maybe as there might be no next element.
       * */
    fun getNextSyncElementFood(id: Long): Maybe<Pair<Food, Food>> = rxMaybe {
        val nextIdElement = database.foodDao.getNextModifiedOrNewAfter(id) ?: return@rxMaybe null
        val nextIdElemReferenceId = nextIdElement.referenceId
        if (nextIdElemReferenceId == null) {
            nextIdElement to nextIdElement
        } else {
            val historic = database.foodDao.getCurrentFromHistoric(nextIdElemReferenceId) ?: return@rxMaybe null
            historic to nextIdElement
        }
    }

    fun getFoodData(): Single<List<Food>> = rxSingle {
        database.foodDao.getFoodData()
    }

    suspend fun getLastFoodId(): Long? =
        database.foodDao.getLastId()

    // BOLUS
    suspend fun getBolusByNSId(nsId: String): Bolus? =
        database.bolusDao.getByNSId(nsId)

    /*
      * returns a Pair of the next entity to sync and the ID of the "update".
      * The update id might either be the entry id itself if it is a new entry - or the id
      * of the update ("historic") entry. The sync counter should be incremented to that id if it was synced successfully.
      *
      * It is a Maybe as there might be no next element.
      * */
    fun getNextSyncElementBolus(id: Long): Maybe<Pair<Bolus, Bolus>> = rxMaybe {
        val nextIdElement = database.bolusDao.getNextModifiedOrNewAfterExclude(id, Bolus.Type.PRIMING) ?: return@rxMaybe null
        val nextIdElemReferenceId = nextIdElement.referenceId
        if (nextIdElemReferenceId == null) {
            nextIdElement to nextIdElement
        } else {
            val historic = database.bolusDao.getCurrentFromHistoric(nextIdElemReferenceId)
            historic?.let { it to nextIdElement }
        }
    }

    fun getNewestBolus(): Maybe<Bolus> = rxMaybe {
        database.bolusDao.getLastBolusRecord()
    }

    fun getLastBolusRecordOfType(type: Bolus.Type): Maybe<Bolus> = rxMaybe {
        database.bolusDao.getLastBolusRecordOfType(type)
    }

    fun getOldestBolus(): Maybe<Bolus> = rxMaybe {
        database.bolusDao.getOldestBolusRecord()
    }

    suspend fun getBolusesDataFromTime(timestamp: Long, ascending: Boolean): List<Bolus> =
        database.bolusDao.getBolusesFromTime(timestamp).reversedIf(!ascending)

    fun getBolusesDataFromTimeToTime(from: Long, to: Long, ascending: Boolean): Single<List<Bolus>> = rxSingle {
        val result = database.bolusDao.getBolusesFromTime(from, to)
        if (!ascending) result.reversed() else result
    }

    suspend fun getBolusesIncludingInvalidFromTime(timestamp: Long, ascending: Boolean): List<Bolus> =
        database.bolusDao.getBolusesIncludingInvalidFromTime(timestamp).reversedIf(!ascending)

    suspend fun getLastBolusId(): Long? =
        database.bolusDao.getLastId()
    // CARBS

    suspend fun getCarbsByNSId(nsId: String): Carbs? =
        database.carbsDao.getByNSId(nsId)

    private fun expandCarbs(carbs: Carbs): List<Carbs> =
        if (carbs.duration == 0L) {
            listOf(carbs)
        } else {
            var remainingCarbs = carbs.amount
            val ticks = (carbs.duration / 1000 / 60 / 15).coerceAtLeast(1L)
            (0 until ticks).map {
                val carbTime = carbs.timestamp + it * 15 * 60 * 1000
                val smallCarbAmount = (1.0 * remainingCarbs / (ticks - it)).roundToInt() //on last iteration (ticks-i) is 1 -> smallCarbAmount == remainingCarbs
                remainingCarbs -= smallCarbAmount.toLong()
                Carbs(timestamp = carbTime, amount = smallCarbAmount.toDouble(), duration = 0)
            }.filter { it.amount != 0.0 }
        }

    /*
      * returns a Pair of the next entity to sync and the ID of the "update".
      * The update id might either be the entry id itself if it is a new entry - or the id
      * of the update ("historic") entry. The sync counter should be incremented to that id if it was synced successfully.
      *
      * It is a Maybe as there might be no next element.
      * */
    fun getNextSyncElementCarbs(id: Long): Maybe<Pair<Carbs, Carbs>> = rxMaybe {
        val nextIdElement = database.carbsDao.getNextModifiedOrNewAfter(id) ?: return@rxMaybe null
        val nextIdElemReferenceId = nextIdElement.referenceId
        if (nextIdElemReferenceId == null) {
            nextIdElement to nextIdElement
        } else {
            val historic = database.carbsDao.getCurrentFromHistoric(nextIdElemReferenceId)
            historic?.let { it to nextIdElement }
        }
    }

    fun getLastCarbs(): Maybe<Carbs> = rxMaybe {
        database.carbsDao.getLastCarbsRecord()
    }

    fun getOldestCarbs(): Maybe<Carbs> = rxMaybe {
        database.carbsDao.getOldestCarbsRecord()
    }

    suspend fun getCarbsDataFromTime(timestamp: Long, ascending: Boolean): List<Carbs> =
        database.carbsDao.getCarbsFromTime(timestamp).reversedIf(!ascending)

    fun getCarbsDataFromTimeExpanded(timestamp: Long, ascending: Boolean): Single<List<Carbs>> = rxSingle {
        val data = database.carbsDao.getCarbsFromTimeExpandable(timestamp)
        val expanded = data.map(::expandCarbs).flatten()
        val filtered = expanded.filter { it.timestamp >= timestamp }
        if (!ascending) filtered.reversed() else filtered
    }

    fun getCarbsDataFromTimeNotExpanded(timestamp: Long, ascending: Boolean): Single<List<Carbs>> = rxSingle {
        val result = database.carbsDao.getCarbsFromTimeExpandable(timestamp)
        if (!ascending) result.reversed() else result
    }

    fun getCarbsDataFromTimeToTimeExpanded(from: Long, to: Long, ascending: Boolean): Single<List<Carbs>> = rxSingle {
        val data = database.carbsDao.getCarbsFromTimeToTimeExpandable(from, to)
        val expanded = data.map(::expandCarbs).flatten()
        val filtered = expanded.filter { it.timestamp in from..to }
        val sorted = filtered.sortedBy { it.timestamp }
        if (!ascending) sorted.reversed() else sorted
    }

    suspend fun getCarbsIncludingInvalidFromTime(timestamp: Long, ascending: Boolean): List<Carbs> =
        database.carbsDao.getCarbsIncludingInvalidFromTime(timestamp).reversedIf(!ascending)

    suspend fun getLastCarbsId(): Long? =
        database.carbsDao.getLastId()

    // BOLUS CALCULATOR RESULT
    suspend fun findBolusCalculatorResultByNSId(nsId: String): BolusCalculatorResult? =
        database.bolusCalculatorResultDao.findByNSId(nsId)

    /*
      * returns a Pair of the next entity to sync and the ID of the "update".
      * The update id might either be the entry id itself if it is a new entry - or the id
      * of the update ("historic") entry. The sync counter should be incremented to that id if it was synced successfully.
      *
      * It is a Maybe as there might be no next element.
      * */
    fun getNextSyncElementBolusCalculatorResult(id: Long): Maybe<Pair<BolusCalculatorResult, BolusCalculatorResult>> = rxMaybe {
        val nextIdElement = database.bolusCalculatorResultDao.getNextModifiedOrNewAfter(id) ?: return@rxMaybe null
        val nextIdElemReferenceId = nextIdElement.referenceId
        if (nextIdElemReferenceId == null) {
            nextIdElement to nextIdElement
        } else {
            val historic = database.bolusCalculatorResultDao.getCurrentFromHistoric(nextIdElemReferenceId) ?: return@rxMaybe null
            historic to nextIdElement
        }
    }

    suspend fun getBolusCalculatorResultsDataFromTime(timestamp: Long, ascending: Boolean): List<BolusCalculatorResult> =
        database.bolusCalculatorResultDao.getBolusCalculatorResultsFromTime(timestamp).reversedIf(!ascending)

    suspend fun getBolusCalculatorResultsIncludingInvalidFromTime(timestamp: Long, ascending: Boolean): List<BolusCalculatorResult> =
        database.bolusCalculatorResultDao.getBolusCalculatorResultsIncludingInvalidFromTime(timestamp).reversedIf(!ascending)

    suspend fun getLastBolusCalculatorResultId(): Long? =
        database.bolusCalculatorResultDao.getLastId()

    // DEVICE STATUS
    fun insert(deviceStatus: DeviceStatus) {
        database.deviceStatusDao.insert(deviceStatus)
        changeSubject.onNext(mutableListOf(deviceStatus)) // Not TraceableDao
    }

    /*
       * returns a Pair of the next entity to sync and the ID of the "update".
       * The update id might either be the entry id itself if it is a new entry - or the id
       * of the update ("historic") entry. The sync counter should be incremented to that id if it was synced successfully.
       *
       * It is a Maybe as there might be no next element.
       * */

    fun getNextSyncElementDeviceStatus(id: Long): Maybe<DeviceStatus> = rxMaybe {
        database.deviceStatusDao.getNextModifiedOrNewAfter(id)
    }

    suspend fun getLastDeviceStatusId(): Long? =
        database.deviceStatusDao.getLastId()

    // TEMPORARY BASAL
    suspend fun findTemporaryBasalByNSId(nsId: String): TemporaryBasal? =
        database.temporaryBasalDao.findByNSId(nsId)

    /*
        * returns a Pair of the next entity to sync and the ID of the "update".
        * The update id might either be the entry id itself if it is a new entry - or the id
        * of the update ("historic") entry. The sync counter should be incremented to that id if it was synced successfully.
        *
        * It is a Maybe as there might be no next element.
        * */

    fun getNextSyncElementTemporaryBasal(id: Long): Maybe<Pair<TemporaryBasal, TemporaryBasal>> = rxMaybe {
        val nextIdElement = database.temporaryBasalDao.getNextModifiedOrNewAfter(id) ?: return@rxMaybe null
        val nextIdElemReferenceId = nextIdElement.referenceId
        if (nextIdElemReferenceId == null) {
            nextIdElement to nextIdElement
        } else {
            val historic = database.temporaryBasalDao.getCurrentFromHistoric(nextIdElemReferenceId)
            historic?.let { it to nextIdElement }
        }
    }

    fun getTemporaryBasalActiveAt(timestamp: Long): Maybe<TemporaryBasal> = rxMaybe {
        database.temporaryBasalDao.getTemporaryBasalActiveAt(timestamp)
    }

    fun getTemporaryBasalsActiveBetweenTimeAndTime(from: Long, to: Long): Single<List<TemporaryBasal>> = rxSingle {
        database.temporaryBasalDao.getTemporaryBasalActiveBetweenTimeAndTime(from, to)
    }

    suspend fun getTemporaryBasalsStartingFromTime(timestamp: Long, ascending: Boolean): List<TemporaryBasal> =
        database.temporaryBasalDao.getTemporaryBasalDataFromTime(timestamp).reversedIf(!ascending)

    fun getTemporaryBasalsStartingFromTimeToTime(from: Long, to: Long, ascending: Boolean): Single<List<TemporaryBasal>> = rxSingle {
        val result = database.temporaryBasalDao.getTemporaryBasalStartingFromTimeToTime(from, to)
        if (!ascending) result.reversed() else result
    }

    suspend fun getTemporaryBasalsStartingFromTimeIncludingInvalid(timestamp: Long, ascending: Boolean): List<TemporaryBasal> =
        database.temporaryBasalDao.getTemporaryBasalDataIncludingInvalidFromTime(timestamp).reversedIf(!ascending)

    fun getOldestTemporaryBasalRecord(): Maybe<TemporaryBasal> = rxMaybe {
        database.temporaryBasalDao.getOldestRecord()
    }

    suspend fun getLastTemporaryBasalId(): Long? =
        database.temporaryBasalDao.getLastId()

    // EXTENDED BOLUS
    suspend fun findExtendedBolusByNSId(nsId: String): ExtendedBolus? =
        database.extendedBolusDao.findByNSId(nsId)

    /*
       * returns a Pair of the next entity to sync and the ID of the "update".
       * The update id might either be the entry id itself if it is a new entry - or the id
       * of the update ("historic") entry. The sync counter should be incremented to that id if it was synced successfully.
       *
       * It is a Maybe as there might be no next element.
       * */

    fun getNextSyncElementExtendedBolus(id: Long): Maybe<Pair<ExtendedBolus, ExtendedBolus>> = rxMaybe {
        val nextIdElement = database.extendedBolusDao.getNextModifiedOrNewAfter(id) ?: return@rxMaybe null
        val nextIdElemReferenceId = nextIdElement.referenceId
        if (nextIdElemReferenceId == null) {
            nextIdElement to nextIdElement
        } else {
            val historic = database.extendedBolusDao.getCurrentFromHistoric(nextIdElemReferenceId)
            historic?.let { it to nextIdElement }
        }
    }

    fun getExtendedBolusActiveAt(timestamp: Long): Maybe<ExtendedBolus> = rxMaybe {
        database.extendedBolusDao.getExtendedBolusActiveAt(timestamp)
    }

    suspend fun getExtendedBolusesStartingFromTime(timestamp: Long, ascending: Boolean): List<ExtendedBolus> =
        database.extendedBolusDao.getExtendedBolusesStartingFromTime(timestamp).reversedIf(!ascending)

    fun getExtendedBolusesStartingFromTimeToTime(start: Long, end: Long, ascending: Boolean): Single<List<ExtendedBolus>> = rxSingle {
        val result = database.extendedBolusDao.getExtendedBolusDataFromTimeToTime(start, end)
        if (!ascending) result.reversed() else result
    }

    suspend fun getExtendedBolusStartingFromTimeIncludingInvalid(timestamp: Long, ascending: Boolean): List<ExtendedBolus> =
        database.extendedBolusDao.getExtendedBolusDataIncludingInvalidFromTime(timestamp).reversedIf(!ascending)

    fun getOldestExtendedBolusRecord(): Maybe<ExtendedBolus> = rxMaybe {
        database.extendedBolusDao.getOldestRecord()
    }

    suspend fun getLastExtendedBolusId(): Long? =
        database.extendedBolusDao.getLastId()

    // TotalDailyDose
    fun getLastTotalDailyDoses(count: Int, ascending: Boolean): Single<List<TotalDailyDose>> = rxSingle {
        val result = database.totalDailyDoseDao.getLastTotalDailyDoses(count)
        if (!ascending) result.reversed() else result
    }

    fun getCalculatedTotalDailyDose(timestamp: Long): Maybe<TotalDailyDose> = rxMaybe {
        database.totalDailyDoseDao.findByTimestamp(timestamp, InterfaceIDs.PumpType.CACHE)
    }

    // HEART RATES

    fun getHeartRatesFromTime(timeMillis: Long): Single<List<HeartRate>> = rxSingle {
        database.heartRateDao.getFromTime(timeMillis)
    }

    fun getHeartRatesFromTimeToTime(startMillis: Long, endMillis: Long): Single<List<HeartRate>> = rxSingle {
        database.heartRateDao.getFromTimeToTime(startMillis, endMillis)
    }

    fun getStepsCountFromTime(timeMillis: Long): Single<List<StepsCount>> = rxSingle {
        database.stepsCountDao.getFromTime(timeMillis)
    }

    suspend fun getStepsCountFromTimeToTime(startMillis: Long, endMillis: Long): List<StepsCount> =
        database.stepsCountDao.getFromTimeToTime(startMillis, endMillis)

    suspend fun getLastStepsCountFromTimeToTime(startMillis: Long, endMillis: Long): StepsCount? =
        database.stepsCountDao.getLastStepsCountFromTimeToTime(startMillis, endMillis)

    suspend fun collectNewEntriesSince(since: Long, until: Long, limit: Int, offset: Int) = NewEntries(
        apsResults = database.apsResultDao.getNewEntriesSince(since, until, limit, offset),
        bolusCalculatorResults = database.bolusCalculatorResultDao.getNewEntriesSince(since, until, limit, offset),
        boluses = database.bolusDao.getNewEntriesSince(since, until, limit, offset),
        carbs = database.carbsDao.getNewEntriesSince(since, until, limit, offset),
        effectiveProfileSwitches = database.effectiveProfileSwitchDao.getNewEntriesSince(since, until, limit, offset),
        extendedBoluses = database.extendedBolusDao.getNewEntriesSince(since, until, limit, offset),
        glucoseValues = database.glucoseValueDao.getNewEntriesSince(since, until, limit, offset),
        runningModes = database.runningModeDao.getNewEntriesSince(since, until, limit, offset),
        preferencesChanges = database.preferenceChangeDao.getNewEntriesSince(since, until, limit, offset),
        profileSwitches = database.profileSwitchDao.getNewEntriesSince(since, until, limit, offset),
        temporaryBasals = database.temporaryBasalDao.getNewEntriesSince(since, until, limit, offset),
        temporaryTarget = database.temporaryTargetDao.getNewEntriesSince(since, until, limit, offset),
        therapyEvents = database.therapyEventDao.getNewEntriesSince(since, until, limit, offset),
        totalDailyDoses = database.totalDailyDoseDao.getNewEntriesSince(since, until, limit, offset),
        versionChanges = database.versionChangeDao.getNewEntriesSince(since, until, limit, offset),
        heartRates = database.heartRateDao.getNewEntriesSince(since, until, limit, offset),
        stepsCount = database.stepsCountDao.getNewEntriesSince(since, until, limit, offset),
    )

    fun getApsResultCloseTo(timestamp: Long): Maybe<APSResult> = rxMaybe {
        database.apsResultDao.getApsResult(timestamp - 5 * 60 * 1000, timestamp)
    }

    fun getApsResults(start: Long, end: Long): Single<List<APSResult>> = rxSingle {
        database.apsResultDao.getApsResults(start, end)
    }

    /**
     * Clean up Flow scope and release resources
     *
     * NOTE: AppRepository is a singleton that typically lives for the entire app lifecycle.
     * This method is primarily useful for:
     * - Unit/integration tests to properly clean up between test runs
     * - Explicit app shutdown scenarios
     *
     * The scope will be automatically cleaned up when the app process terminates.
     *   @Test
     *   fun myTest() {
     *       repository.use { repo ->
     *           // test code
     *       } // automatically calls close()
     *   }
     */
    override fun close() {
        repositoryScope.cancel()
    }

    fun <T> Iterable<T>.reversedIf(reverse: Boolean): List<T> = if (reverse) this.reversed() else this.toList()
}