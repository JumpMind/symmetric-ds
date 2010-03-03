package org.jumpmind.symmetric.server

class TriggerMapping {

  String triggerId
  String catalogName
  String schemaName
  String tableName
  String channelId
  int order = 100
  boolean selected

  def getFullTableName() {
    def name = catalogName ? catalogName + "." : ''
    name = schemaName ? name + schemaName + "." : name
    name = name + tableName
    return name
  }

}
