/*
 * =====================================================================================
 *
 *       Filename:  conn.h
 *
 *    Description:  make dbconnection easier
 *
 *        Version:  1.0
 *        Created:  12/21/2014 08:53:35
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  zhifan (Zhihui Fan), zhihuifan@163.com
 *   Organization:
 *
 * =====================================================================================
 */

#ifndef CONN_INC
#define CONN_INC
#include <occi.h>
#include <string>
#include <memory>
namespace databus {
  using namespace oracle::occi;
  using std::string;
  class DBConn {
   public:
    DBConn(string& user, string& passwd, string& db, string& desc);
    ~DBConn();
    Connection* conn();

   private:
    Environment* env_;
    Connection* conn_;
  };

  // Note: this function used glogal variable streamconf, which get init in
  // initstream function
  std::shared_ptr<DBConn> makeConnToSrc(string& desc);
}
#endif /* ----- ifndef CONN_INC  ----- */
