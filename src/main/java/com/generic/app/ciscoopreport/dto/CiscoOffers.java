/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.generic.app.ciscoopreport.dto;

public class CiscoOffers {

	private int id;
	private String uuid;
	private int version;
	private int created;
	private int last_update;
	private String currency;
	private String userid;
	private int points;

	public CiscoOffers(int id, String uuid, int version, int created,
			int last_update, String currency, String userid, int points) {
		this.id = id;
		this.uuid = uuid;
		this.version = version;
		this.created = created;
		this.last_update = last_update;
		this.currency = currency;
		this.userid = userid;
		this.points = points;
	}

	public Object getPoints() {
		return points;
	}

	public void setPoints(int points) {
		this.points = points;
	}

	public String getUserid() {
		return userid;
	}

	public void setUserid(String userid) {
		this.userid = userid;
	}

	public String getCurrency() {
		return currency;
	}

	public void setCurrency(String currency) {
		this.currency = currency;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public int getCreated() {
		return created;
	}

	public void setCreated(int created) {
		this.created = created;
	}

	public int getLast_update() {
		return last_update;
	}

	public void setLast_update(int last_update) {
		this.last_update = last_update;
	}

}
