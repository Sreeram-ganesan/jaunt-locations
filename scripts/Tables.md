### **Database Table Overview**
This is the summary of tables in the database along with their row counts and column details.

---

#### **1. User and Organization Data**
| Table Name      | Row Count | Columns                                                                                                                                                                       |
|------------------|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `t_usercategory` | 294       | `usercategoryid`, `userid`, `categoryid`, `rating`, `active`, `isdeleted`, `createdby`, `createdon`, `updatedby`, `updatedon`                                                |
| `t_organization` | 1         | `organizationid`, `active`, `isdeleted`, `logourl`, `termsandconditions`, `privatepolicy`, `name`, `description`                                                            |
| `t_user`         | 112       | `userid`, `organizationid`, `active`, `isdeleted`, `createdby`, `createdon`, `updatedby`, `updatedon`, `phonenumber`, `firstname`, `middlename`, `lastname`, `emailid`        |
| `t_userlogin`    | 0         | `userloginid`, `emailid`, `password`, `salt`, `active`, `isdeleted`, `createdby`, `createdon`, `updatedby`, `updatedon`                                                     |
| `t_feedback`     | 30        | `feedbackid`, `userid`, `contentid`, `ratings`, `comments`, `active`, `isdeleted`, `createdby`, `createdon`, `updatedby`, `updatedon`                                       |
| `t_trips`        | 111       | `tripid`, `userid`, `locationid`, `startdate`, `enddate`, `totaladult`, `totalchild`, `triptime`, `preferences`, `active`, `isdeleted`, `createdby`, `createdon`            |

---

#### **2. Location and Geographic Data**
| Table Name           | Row Count | Columns                                                                                                                                                                   |
|-----------------------|-----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `t_location`          | 311       | `locationid`, `parentlocationid`, `locationname`, `countyname`, `latitude`, `longitude`, `timezone`, `active`, `isdeleted`, `createdby`, `createdon`, `updatedby`, `updatedon` |
| `t_locationbackup`    | 973       | `locationid`, `parentlocationid`, `locationname`, `countyname`, `latitude`, `longitude`, `timezone`, `zipcodes`, `active`, `isdeleted`, `createdby`, `createdon`         |
| `t_primarylocations`  | 385       | `primarylocationid`, `latitude`, `longitude`, `Short Story`, `Key Words`, `address`, `borough`, `neighborhood`, `Primary Location`, `history`, `external_audiourl`       |
| `t_googlemaps_data`   | 6096      | `googlemaps_data`, `latitude`, `longitude`, `address`, `phone`, `website`, `categories`, `rating`, `reviews`, `working_days`, `place_id`, `borough`, `timezone`          |
| `t_restaurants`       | 6263      | `googlemaps_data`, `latitude`, `longitude`, `address`, `phone`, `website`, `categories`, `rating`, `reviews`, `working_days`, `place_id`, `borough`, `timezone`          |

---

#### **3. Content and Media Data**
| Table Name                  | Row Count | Columns                                                                                                                                                               |
|-----------------------------|-----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `t_content`                 | 3636      | `contentid`, `title`, `description`, `keywords`, `latitude`, `longitude`, `images`, `history`, `facts`, `type`, `active`, `isdeleted`, `createdon`, `updatedon`        |
| `t_content_clone`           | 3644      | `contentid`, `title`, `subtitle`, `keywords`, `images`, `latitude`, `longitude`, `facts`, `long_story`, `short_story`, `audiourl`, `active`, `isdeleted`, `createdon`  |
| `t_content_final_type1`     | 3636      | `contentid`, `title_story_combined`, `preferences`, `history`, `longitude`, `latitude`, `keywords`, `facts`, `images`, `audiourl`, `updatedon`, `isprocessed`          |
| `t_content_final_type2`     | 3393      | `contentid`, `title`, `keywords`, `images`, `description`, `latitude`, `longitude`, `facts`, `preferences`, `isprocessed`, `createdon`, `updatedon`, `isdeleted`       |
| `t_category`                | 15        | `categoryid`, `title`, `description`, `imageurl`, `active`, `isdeleted`, `createdon`, `updatedon`                                                                     |

---

#### **4. External Data Sources**
| Table Name             | Row Count | Columns                                                                                         |
|-------------------------|-----------|-------------------------------------------------------------------------------------------------|
| `t_atlasobscura_data`   | 9884      | `atlasobscura_dataid`, `title`, `subtitle`, `coordinates.lat`, `coordinates.lng`, `description` |
| `t_tripadvisor_data`    | 210       | `tripadvisor_data_id`, `ranking`, `rating`, `reviews`, `cuisines`, `features`, `hours`         |
| `t_hmdb_data`           | 13841     | `hmdb_dataid`, `title`, `subtitle`, `latitude`, `longitude`, `description`, `markerid`         |

---

#### **5. Miscellaneous**
| Table Name                 | Row Count | Columns                                                                                   |
|----------------------------|-----------|-------------------------------------------------------------------------------------------|
| `t_datascraping`           | 0         | `datascrapingid`, `reviews`, `rating`, `title`, `type`, `website`, `phone`, `address`     |
| `t_stagecontent`           | 3258      | `contentid`, `title`, `keywords`, `images`, `latitude`, `longitude`, `description`        |
| `t_carousel`               | 12        | `id`, `title`, `description`, `imageurl`, `link`, `type`, `active`                        |
| `spatial_ref_sys`          | 8500      | `srid`, `auth_name`, `proj4text`                                                         |

---

This structure organizes the tables into relevant categories for easier understanding and accessibility. Let me know if further adjustments are needed!