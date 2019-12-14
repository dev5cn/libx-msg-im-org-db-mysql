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
#include "XmsgImOrgDeptUsrCollOper.h"
#include "XmsgImOrgUsrCollOper.h"
#include "XmsgImOrgDb.h"

XmsgImOrgDeptUsrCollOper* XmsgImOrgDeptUsrCollOper::inst = new XmsgImOrgDeptUsrCollOper();

XmsgImOrgDeptUsrCollOper::XmsgImOrgDeptUsrCollOper()
{

}

XmsgImOrgDeptUsrCollOper* XmsgImOrgDeptUsrCollOper::instance()
{
	return XmsgImOrgDeptUsrCollOper::inst;
}

bool XmsgImOrgDeptUsrCollOper::insert(SptrOrgDeptUsr coll)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool, coll: %s", coll->toString().c_str())
		return false;
	}
	bool ret = this->insert(conn, coll);
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgImOrgDeptUsrCollOper::insert(void* conn, SptrOrgDeptUsr coll)
{
	string sql;
	SPRINTF_STRING(&sql, "insert into %s values (?, ?, ?, ?, ?, ?, ?, ?)", XmsgImOrgDb::xmsgImOrgDeptUsrColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addVarchar(coll->dcgt->toString()) 
	->addVarchar(coll->ucgt->toString()) 
	->addVarchar(coll->name) 
	->addBool(coll->enable) 
	->addBlob(coll->info->SerializeAsString()) 
	->addLong(coll->ver) 
	->addDateTime(coll->gts) 
	->addDateTime(coll->uts);
	return MysqlMisc::sql((MYSQL*) conn, req, [coll](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("insert into %s.%s failed, coll: %s, ret: %04X, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImOrgDb::xmsgImOrgDeptUsrColl.c_str(), coll->toString().c_str(), ret, desc.c_str())
			return;
		}
		LOG_TRACE("insert into %s.%s successful, coll: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImOrgDb::xmsgImOrgDeptUsrColl.c_str(), coll->toString().c_str())
	});
}

bool XmsgImOrgDeptUsrCollOper::insert(SptrOrgUsr usr, SptrOrgDeptUsr deptUsr)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool.")
		return false;
	}
	if (!MysqlMisc::start(conn))
	{
		LOG_ERROR("start transaction failed, err: %s, coll: %s", ::mysql_error(conn), deptUsr->toString().c_str())
		MysqlConnPool::instance()->relConn(conn, false);
		return false;
	}
	if (!XmsgImOrgUsrCollOper::instance()->insert(conn, usr))
	{
		LOG_ERROR("insert into %s failed, usr: %s, deptUsr: %s", XmsgImOrgDb::xmsgImOrgUsrColl.c_str(), usr->toString().c_str(), deptUsr->toString().c_str())
		MysqlMisc::rollBack(conn);
		MysqlConnPool::instance()->relConn(conn, false);
		return false;
	}
	if (!XmsgImOrgDeptUsrCollOper::instance()->insert(conn, deptUsr))
	{
		LOG_ERROR("insert into %s failed, usr: %s, deptUsr: %s", XmsgImOrgDb::xmsgImOrgDeptUsrColl.c_str(), usr->toString().c_str(), deptUsr->toString().c_str())
		MysqlMisc::rollBack(conn);
		MysqlConnPool::instance()->relConn(conn, false);
		return false;
	}
	if (!MysqlMisc::commit(conn))
	{
		LOG_ERROR("commit transaction failed, err: %s, coll: %s", ::mysql_error(conn), deptUsr->toString().c_str())
		MysqlMisc::rollBack(conn);
		MysqlConnPool::instance()->relConn(conn, false);
		return false;
	}
	MysqlConnPool::instance()->relConn(conn);
	return true;
}

bool XmsgImOrgDeptUsrCollOper::update(SptrOrgDeptUsr coll)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool, coll: %s", coll->toString().c_str())
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "update %s set name = ?, enable = ?, info = ?, ver = ?, uts = ? where dcgt = ? and ucgt = ?", XmsgImOrgDb::xmsgImOrgDeptUsrColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addVarchar(coll->name) 
	->addBool(coll->enable) 
	->addBlob(coll->info->SerializeAsString()) 
	->addLong(coll->ver) 
	->addDateTime(coll->uts) 
	->addVarchar(coll->dcgt->toString()) 
	->addVarchar(coll->ucgt->toString());
	ullong sts = DateMisc::dida();
	bool ret = MysqlMisc::sql((MYSQL*) conn, req, [coll, sts](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("update table %s.%s failed, elap: %dms, coll: %s, ret: %04X, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImOrgDb::xmsgImOrgDeptUsrColl.c_str(), DateMisc::elapDida(sts), coll->toString().c_str(), ret, desc.c_str())
			return;
		}
		LOG_DEBUG("update table %s.%s successful, elap: %dms, coll: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImOrgDb::xmsgImOrgDeptUsrColl.c_str(), DateMisc::elapDida(sts), coll->toString().c_str())
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgImOrgDeptUsrCollOper::load(void (*loadCb)(SptrOrgDeptUsr coll))
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool.")
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "select * from %s", XmsgImOrgDb::xmsgImOrgDeptUsrColl.c_str())
	bool ret = MysqlMisc::query(conn, sql, [loadCb](int ret, const string& desc, bool more, int rows, shared_ptr<MysqlResultRow> row)
	{
		if (ret != 0) 
		{
			LOG_ERROR("load %s failed, ret: %d, desc: %s", XmsgImOrgDb::xmsgImOrgDeptUsrColl.c_str(), ret, desc.c_str())
			return false;
		}
		if (row == NULL) 
		{
			LOG_DEBUG("table %s no record", XmsgImOrgDb::xmsgImOrgDeptUsrColl.c_str())
			return true;
		}
		auto coll = XmsgImOrgDeptUsrCollOper::instance()->loadOneFromIter(row.get());
		if(coll == nullptr)
		{
			LOG_ERROR("have some one %s format error, row: %s", XmsgImOrgDb::xmsgImOrgDeptUsrColl.c_str(), row->toString().c_str())
			return false; 
		}
		LOG_RECORD("got a %s: %s", XmsgImOrgDb::xmsgImOrgDeptUsrColl.c_str(), coll->toString().c_str())
		loadCb(coll);
		return true;
	});
	if (ret)
	{
		LOG_INFO("x-msg-im-org dept-usr max version is: %llu", XmsgImOrgMgr::instance()->getVer4deptUsr())
	}
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

SptrOrgDeptUsr XmsgImOrgDeptUsrCollOper::loadOneFromIter(void* it)
{
	MysqlResultRow* row = (MysqlResultRow*) it;
	string str;
	if (!row->getStr("dcgt", str))
	{
		LOG_ERROR("can not found field: dcgt")
		return nullptr;
	}
	SptrCgt dcgt = ChannelGlobalTitle::parse(str);
	if (dcgt == nullptr)
	{
		LOG_ERROR("dcgt format error: %s", str.c_str())
		return nullptr;
	}
	if (!row->getStr("ucgt", str))
	{
		LOG_ERROR("can not found field: ucgt")
		return nullptr;
	}
	SptrCgt ucgt = ChannelGlobalTitle::parse(str);
	if (ucgt == nullptr)
	{
		LOG_ERROR("ucgt format error: %s", str.c_str())
		return nullptr;
	}
	bool enable;
	if (!row->getBool("enable", enable))
	{
		LOG_ERROR("can not found field: enable")
		return nullptr;
	}
	if (!row->getBin("info", str))
	{
		LOG_ERROR("can not found field: info")
		return nullptr;
	}
	shared_ptr<XmsgImOrgNodeInfo> info(new XmsgImOrgNodeInfo());
	if (!info->ParseFromString(str))
	{
		LOG_ERROR("XmsgImOrgNodeInfo format error, dcgt: %s, ucgt: %s", dcgt->toString().c_str(), ucgt->toString().c_str())
		return nullptr;
	}
	ullong ver;
	if (!row->getLong("ver", ver))
	{
		LOG_ERROR("can not found field: ver, dcgt: %s, ucgt: %s", dcgt->toString().c_str(), ucgt->toString().c_str())
		return nullptr;
	}
	if (ver == 0)
	{
		LOG_FAULT("ver can not be zero, dcgt: %s, ucgt: %s", dcgt->toString().c_str(), ucgt->toString().c_str())
		return nullptr;
	}
	ullong gts;
	if (!row->getLong("gts", gts))
	{
		LOG_ERROR("can not found field: gts")
		return nullptr;
	}
	ullong uts;
	if (!row->getLong("uts", uts))
	{
		LOG_ERROR("can not found field: uts")
		return nullptr;
	}
	SptrOrgDeptUsr coll(new XmsgImOrgDeptUsrColl());
	coll->dcgt = dcgt;
	coll->ucgt = ucgt;
	coll->enable = enable;
	coll->info = info;
	coll->ver = ver;
	coll->gts = gts;
	coll->uts = uts;
	return coll;
}

XmsgImOrgDeptUsrCollOper::~XmsgImOrgDeptUsrCollOper()
{

}

