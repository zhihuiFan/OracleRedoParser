#include <memory>
#include "dbconn.h"
#include "stream.h"

namespace databus {
  DBConn::DBConn(string& user, string& passwd, string& db, string& desc) {
    env_ = Environment::createEnvironment(Environment::DEFAULT);
    conn_ = env_->createConnection(user, passwd, db);
    // set client info with desc
  }

  DBConn::~DBConn() {
    if (env_) delete env_;
    if (conn_) delete conn_;
  }

  Connection* conn() { return conn_; }

  // Note: this function used glogal variable streamconf, which get init in
  // initstream function
  std::shared_ptr<DBConn> makeConnToSrc(string& desc) {
    return std::shared_ptr<DBConn>(new DBConn(streamconf->getString("srcUser"),
                                              streamconf->getString("srcPass"),
                                              streamconf->getString("srcDB")),
                                   desc);
  }
}
