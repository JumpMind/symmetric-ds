package org.jumpmind.db.io;

public class Issue {

	private String id;
	
	private String version;
	
	private String project;
	
	private String priority;
	
	private String summary;
	
	private String category;
	
	private String tag;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getProject() {
		return project;
	}

	public void setProject(String project) {
		this.project = project;
	}
	
	public int getPriorityAsInt() {
		if (priority != null)
		{
			if (priority.equalsIgnoreCase("low"))
				return 1;
			else if (priority.equalsIgnoreCase("medium"))
				return 2;
			else if (priority.equalsIgnoreCase("high"))
				return 3;
		}
		return -1;
	}

	public String getPriority() {
		return priority;
	}

	public void setPriority(String priority) {
		this.priority = priority;
	}

	public String getSummary() {
		return summary;
	}

	public void setSummary(String summary) {
		this.summary = summary;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}
	
}
