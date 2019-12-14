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
#include "XmsgImOrgDeptCollOper.h"
#include "XmsgImOrgDb.h"

XmsgImOrgDeptCollOper* XmsgImOrgDeptCollOper::inst = new XmsgImOrgDeptCollOper();

XmsgImOrgDeptCollOper::XmsgImOrgDeptCollOper()
{

}

XmsgImOrgDeptCollOper* XmsgImOrgDeptCollOper::instance()
{
	return XmsgImOrgDeptCollOper::inst;
}

bool XmsgImOrgDeptCollOper::insert(SptrOrgDept coll)
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

bool XmsgImOrgDeptCollOper::insert(void* conn, SptrOrgDept coll)
{
	string sql;
	SPRINTF_STRING(&sql, "insert into %s values (?, ?, ?, ?, ?, ?, ?, ?)", XmsgImOrgDb::xmsgImOrgDeptColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addVarchar(coll->cgt->toString()) 
	->addVarchar(coll->pcgt == nullptr ? "" : coll->pcgt->toString()) 
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
			LOG_ERROR("insert into %s.%s failed, coll: %s, ret: %04X, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImOrgDb::xmsgImOrgDeptColl.c_str(), coll->toString().c_str(), ret, desc.c_str())
			return;
		}
		LOG_TRACE("insert into %s.%s successful, coll: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImOrgDb::xmsgImOrgDeptColl.c_str(), coll->toString().c_str())
	});
}

bool XmsgImOrgDeptCollOper::update(SptrOrgDept coll)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool, coll: %s", coll->toString().c_str())
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "update %s set pcgt = ?, name = ?, enable = ?, info = ?, ver = ?, uts = ? where cgt = ?", XmsgImOrgDb::xmsgImOrgDeptColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addVarchar(coll->pcgt == nullptr ? "" : coll->pcgt->toString()) 
	->addVarchar(coll->name) 
	->addBool(coll->enable) 
	->addBlob(coll->info->SerializeAsString()) 
	->addLong(coll->ver) 
	->addDateTime(coll->uts) 
	->addVarchar(coll->cgt->toString());
	ullong sts = DateMisc::dida();
	bool ret = MysqlMisc::sql((MYSQL*) conn, req, [coll, sts](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("update table %s.%s failed, elap: %dms, coll: %s, ret: %04X, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImOrgDb::xmsgImOrgDeptColl.c_str(), DateMisc::elapDida(sts), coll->toString().c_str(), ret, desc.c_str())
			return;
		}
		LOG_DEBUG("update table %s.%s successful, elap: %dms, coll: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImOrgDb::xmsgImOrgDeptColl.c_str(), DateMisc::elapDida(sts), coll->toString().c_str())
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgImOrgDeptCollOper::load(void (*loadCb)(SptrOrgDept coll))
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool.")
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "select * from %s", XmsgImOrgDb::xmsgImOrgDeptColl.c_str())
	int size = 0;
	bool ret = MysqlMisc::query(conn, sql, [loadCb, &size](int ret, const string& desc, bool more, int rows, shared_ptr<MysqlResultRow> row)
	{
		if (ret != 0) 
		{
			LOG_ERROR("load %s failed, ret: %d, desc: %s", XmsgImOrgDb::xmsgImOrgDeptColl.c_str(), ret, desc.c_str())
			return false;
		}
		if (row == NULL) 
		{
			LOG_DEBUG("table %s no record", XmsgImOrgDb::xmsgImOrgDeptColl.c_str())
			return true;
		}
		auto coll = XmsgImOrgDeptCollOper::instance()->loadOneFromIter(row.get());
		if(coll == nullptr)
		{
			LOG_ERROR("have some one %s format error, row: %s", XmsgImOrgDb::xmsgImOrgDeptColl.c_str(), row->toString().c_str())
			return false; 
		}
		LOG_RECORD("got a %s: %s", XmsgImOrgDb::xmsgImOrgDeptColl.c_str(), coll->toString().c_str())
		loadCb(coll);
		++size;
		return true;
	});
	if (ret && size == 0) 
	{
		SptrOrgDept root = this->initRoot(conn);
		if (root == nullptr)
		{
			LOG_FAULT("can not init root node for x-msg-im-org")
			MysqlConnPool::instance()->relConn(conn, false);
			return false;
		}
		XmsgImOrgMgr::instance()->addOrgDept(root); 
		XmsgImOrgMgr::instance()->setRoot(root);
		XmsgImOrgMgr::instance()->setVer4dept(root->ver);
		LOG_INFO("init root node for x-msg-im-org successful, root: %s", root->toString().c_str())
	}
	LOG_INFO("x-msg-im-org dept max version is: %llu", XmsgImOrgMgr::instance()->getVer4dept())
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

SptrOrgDept XmsgImOrgDeptCollOper::loadOneFromIter(void* it)
{
	MysqlResultRow* row = (MysqlResultRow*) it;
	string str;
	if (!row->getStr("cgt", str))
	{
		LOG_ERROR("can not found field: cgt")
		return nullptr;
	}
	SptrCgt cgt = ChannelGlobalTitle::parse(str);
	if (cgt == nullptr)
	{
		LOG_ERROR("cgt format error: %s", str.c_str())
		return nullptr;
	}
	if (!row->getStr("pcgt", str))
	{
		LOG_ERROR("can not found field: pcgt")
		return nullptr;
	}
	SptrCgt pcgt = nullptr;
	if (!str.empty())
	{
		pcgt = ChannelGlobalTitle::parse(str);
		if (pcgt == nullptr)
		{
			LOG_ERROR("pcgt format error: %s", str.c_str())
			return nullptr;
		}
	}
	string name;
	if (!row->getStr("name", name))
	{
		LOG_ERROR("can not found field: name")
		return nullptr;
	}
	if (name.empty())
	{
		LOG_ERROR("name format error: %s", cgt->toString().c_str())
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
		LOG_ERROR("XmsgImOrgNodeInfo format error: %s", cgt->toString().c_str())
		return nullptr;
	}
	ullong ver;
	if (!row->getLong("ver", ver))
	{
		LOG_ERROR("can not found field: ver")
		return nullptr;
	}
	if (ver == 0)
	{
		LOG_FAULT("ver can not be zero, cgt: %s", cgt->toString().c_str())
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
	SptrOrgDept coll(new XmsgImOrgDeptColl());
	coll->cgt = cgt;
	coll->pcgt = pcgt;
	coll->name = name;
	coll->info = info;
	coll->enable = enable;
	coll->ver = ver;
	coll->gts = gts;
	coll->uts = uts;
	return coll;
}

SptrOrgDept XmsgImOrgDeptCollOper::initRoot(void* conn)
{
	SptrOrgDept root(new XmsgImOrgDeptColl());
	root->cgt = ChannelGlobalTitle::genGroup(XmsgImOrgCfg::instance()->cgt);
	root->pcgt = nullptr;
	root->name = "root";
	root->enable = true;
	root->info.reset(new XmsgImOrgNodeInfo());
	root->ver = 1ULL;
	root->gts = DateMisc::nowGmt0();
	root->uts = root->gts;
	return this->insert(conn, root) ? root : nullptr;
}

XmsgImOrgDeptCollOper::~XmsgImOrgDeptCollOper()
{

}

