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

#ifndef XMSGIMORGDEPTUSRCOLLOPER_H_
#define XMSGIMORGDEPTUSRCOLLOPER_H_

#include <libx-msg-im-org-core.h>

class XmsgImOrgDeptUsrCollOper
{
public:
	bool insert(SptrOrgDeptUsr coll); 
	bool insert(void* conn, SptrOrgDeptUsr coll); 
	bool insert(SptrOrgUsr usr, SptrOrgDeptUsr deptUsr); 
	bool update(SptrOrgDeptUsr coll); 
	bool load(void (*loadCb)(SptrOrgDeptUsr coll)); 
	static XmsgImOrgDeptUsrCollOper* instance();
private:
	static XmsgImOrgDeptUsrCollOper* inst;
	SptrOrgDeptUsr loadOneFromIter(void* it); 
	XmsgImOrgDeptUsrCollOper();
	virtual ~XmsgImOrgDeptUsrCollOper();
};

#endif 
