package test

import atomicflow.WorkflowRuntime
import atomicflow.impl.db.DbWorkflowRuntime
import atomicflow.impl.db.DbWorkflowRuntime.DbConfig

class DbWorkflowRuntimeSuite extends WorkflowRuntimeSuite {
  val dbConfig = DbConfig(
    driver = None,
    url = sys.env("DB_URL"),
    user = sys.env("DB_USERNAME"),
    password = sys.env("DB_PASSWORD"),
    poolSize = None
  )

  override def createWorkflowRuntime: WorkflowRuntime = DbWorkflowRuntime(dbConfig)
}
