package simpledb;

import simpledb.page.PageId;

public final class SimpleAction 
{
	private PageId pid;
	private Permissions permission;
	
	public SimpleAction(PageId pid, Permissions permission) 
	{
		this.pid = pid;
		this.permission = permission;
	}

	public PageId getPid() {
		return pid;
	}

	public Permissions getPermission() {
		return permission;
	}
	
	
	
	
}


