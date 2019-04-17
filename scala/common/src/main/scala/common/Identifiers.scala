package common

object Identifiers {
  final val LISTENER = "listener"
  final val USER = "user"
  final val APP_MASTER= "appmaster"
  final val STATE_MASTER = "statemaster"
  final val CLUSTER = "arcon"

  // ArcTask states
  final val ARC_TASK_RUNNING = "running"
  final val ARC_TASK_PENDING = "pending"
  final val ARC_TASK_FINISHED = "finished"
  final val ARC_TASK_KILLED = "killed"
  final val ARC_TASK_TRANSFER_ERROR = "transfer error"

  // ArcApp states
  final val ARC_APP_DEPLOYING = "deploying"
  final val ARC_APP_INITIALIZING = "initializing"
  final val ARC_APP_RUNNING = "running"
  final val ARC_APP_KILLED = "killed"
  final val ARC_APP_FAILED = "failed"
  final val ARC_APP_SUCCEEDED = "succeeded"
}
