API
=====

**Get User Credentials**
----
  Returns json with user credentials to storage.

##### **URL**

  /get_user_credentials

##### **Method:**

  `GET`

##### **URL Params**

| Param  | Description | 
| :--  | :-- | 
| global_id  | user global id | 
| storage_type | storage type e.g. `Ceph` | 
| storage_id | storage id | 
| source_ips | IPs list of provider performing query as string encoded JSON |
| source_hostname | hostname of provider performing query | 
| user_details | detail information of user as string encoded JSON | 

**NOTE:** One of `storage_id`, `storage_type` may be omitted in request.

User details:

* id
* name
* connected_accounts
* alias
* email_list 
    
##### **Success Response:**

* **Code:** 200 OK <br />
  **Content:**
  * POSIX
  ```
  {
      "status": "success",
      "credentials": {
          "uid": 31415
      }
  }
  ```
  * CEPH
  ```
  {
      "status": "success",
      "credentials": {
          "access_key": "ACCESS_KEY",
          "secret_key": "SECRET_KEY"
      }
  }
  ```
  * AMAZON S3
  ```
  {
      "status": "success",
      "credentials": {
          "user_name": "USER",
          "user_key": "KEY"
      }
  }
  ```

##### **Error Response:**

  * **Code:** 422 Unprocessable Entity <br />
    **Content:** `{ "status: "error", "message": "Missing parameter global_id" }`

  OR

  * **Code:** 500 Internal Server Error <br />
    **Content:** `{ "status: "error", "message": "MESSAGE" }`
