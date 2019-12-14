/*
  Copyright 2019 www.dev5.cn, Inc. dev5@qq.com
 
  This file is part of X-MSG-IM.
 
  X-MSG-IM is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  X-MSG-IM is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.
 
  You should have received a copy of the GNU Affero General Public License
  along with X-MSG-IM.  If not, see <https://www.gnu.org/licenses/>.
 */

#include <libmisc-mysql-c.h>
#include "XmsgImOrgDb.h"
#include "XmsgImOrgCfgCollOper.h"
#include "XmsgImOrgUsrCollOper.h"
#include "XmsgImOrgDeptCollOper.h"
#include "XmsgImOrgDeptUsrCollOper.h"

XmsgImOrgDb* XmsgImOrgDb::inst = new XmsgImOrgDb();

string XmsgImOrgDb::xmsgImOrgCfgColl = "tb_x_msg_im_org_cfg"; 
string XmsgImOrgDb::xmsgImOrgDeptColl = "tb_x_msg_im_org_dept"; 
string XmsgImOrgDb::xmsgImOrgDeptUsrColl = "tb_x_msg_im_org_dept_usr"; 
string XmsgImOrgDb::xmsgImOrgUsrColl = "tb_x_msg_im_org_usr"; 

XmsgImOrgDb::XmsgImOrgDb()
{

}

XmsgImOrgDb* XmsgImOrgDb::instance()
{
	return XmsgImOrgDb::inst;
}

bool XmsgImOrgDb::load()
{
	auto& cfg = XmsgImOrgCfg::instance()->cfgPb->mysql();
	if (!MysqlConnPool::instance()->init(cfg.host(), cfg.port(), cfg.db(), cfg.usr(), cfg.password(), cfg.poolsize()))
		return false;
	LOG_INFO("init mysql connection pool successful, host: %s:%d, db: %s", cfg.host().c_str(), cfg.port(), cfg.db().c_str())
	if ("mysql" == XmsgImOrgCfg::instance()->cfgPb->cfgtype() && !this->initCfg())
		return false;
	ullong sts = DateMisc::dida();
	if (!XmsgImOrgUsrCollOper::instance()->load(XmsgImOrgMgr::instance()->loadCb4usr))
		return false;
	LOG_INFO("load %s.%s successful, count: %zu, ver: %llu, elap: %dms", MysqlConnPool::instance()->getDbName().c_str(), XmsgImOrgDb::xmsgImOrgUsrColl.c_str(), XmsgImOrgMgr::instance()->size4usr(), XmsgImOrgMgr::instance()->getVer4usr(), DateMisc::elapDida(sts))
	sts = DateMisc::dida();
	if (!XmsgImOrgDeptCollOper::instance()->load(XmsgImOrgMgr::instance()->loadCb4dept))
		return false;
	LOG_INFO("load %s.%s successful, count: %zu, ver: %llu, elap: %dms", MysqlConnPool::instance()->getDbName().c_str(), XmsgImOrgDb::xmsgImOrgDeptColl.c_str(), XmsgImOrgMgr::instance()->size4dept(), XmsgImOrgMgr::instance()->getVer4dept(), DateMisc::elapDida(sts))
	sts = DateMisc::dida();
	if (!XmsgImOrgDeptUsrCollOper::instance()->load(XmsgImOrgMgr::instance()->loadCb4deptUsr))
		return false;
	LOG_INFO("load %s.%s successful, ver: %llu, elap: %dms", MysqlConnPool::instance()->getDbName().c_str(), XmsgImOrgDb::xmsgImOrgDeptUsrColl.c_str(), XmsgImOrgMgr::instance()->getVer4deptUsr(), DateMisc::elapDida(sts))
	this->abst.reset(new ActorBlockingSingleThread("org-db")); 
	XmsgImOrgSubClientMgr::instance()->init(); 
	return true;
}

void XmsgImOrgDb::future(function<void()> cb)
{
	this->abst->future(cb);
}

bool XmsgImOrgDb::initCfg()
{
	auto coll = XmsgImOrgCfgCollOper::instance()->load();
	if (coll == NULL)
		return false;
	LOG_INFO("got a x-msg-im-org config from db: %s", coll->toString().c_str())
	XmsgImOrgCfg::instance()->cfgPb = coll->cfg;
	return true;
}

XmsgImOrgDb::~XmsgImOrgDb()
{

}

