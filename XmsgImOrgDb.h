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

#ifndef XMSGIMORGDB_H_
#define XMSGIMORGDB_H_

#include <libx-msg-im-org-core.h>
#include <libx-msg-im-org-pb.h>

class XmsgImOrgDb
{
public:
	bool load(); 
	void future(function<void()> cb); 
	static XmsgImOrgDb* instance();
public:
	static string xmsgImOrgCfgColl; 
	static string xmsgImOrgDeptColl; 
	static string xmsgImOrgDeptUsrColl; 
	static string xmsgImOrgUsrColl; 
private:
	shared_ptr<ActorBlockingSingleThread> abst; 
	static XmsgImOrgDb* inst;
	bool initCfg(); 
	XmsgImOrgDb();
	virtual ~XmsgImOrgDb();
};

#endif 
