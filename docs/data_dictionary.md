# Data Dictionary

Every table in the database, with every column explained.

---
## File Structure

```
Vyoma_DataWarehouse/
├─ analytics/
│  ├─ build_courses.py
│  ├─ README.md
│  └─ run_analysis.py
├─ api_collectors/
│  |
│  ├─ attendance.md
│  ├─ attendance.py
│  ├─ course_batches.md
│  ├─ course_batches.py
│  ├─ course_catalogue.md
│  ├─ course_catalogue.py
│  ├─ local_storage_helper.py
│  ├─ README.md
│  ├─ run_course_pipeline.md
│  └─ run_course_pipeline.py
├─ CSV files/
│  └─ README.md
├─ database/
│  ├─ bronze/
│  │  ├─ api/
│  │  │  ├─ attendance_raw.sql
│  │  │  ├─ batches_data_raw.sql
│  │  │  ├─ course_catalogue_data_raw.sql
│  │  │  └─ README.md
│  │  ├─ manual/
│  │  │  ├─ course_lifecycle_raw.sql
│  │  │  ├─ README.md
│  │  │  ├─ student_courses_enrolled_raw.sql
│  │  │  ├─ studentexport_raw.sql
│  │  │  └─ unresolved_students_raw.sql
│  │  ├─ webhook/
│  │  │  ├─ failed_events.sql
│  │  │  ├─ README.md
│  │  │  └─ webhook_events.sql
│  │  └─ README.md
│  ├─ gold/
│  │  ├─ api/
│  │  │  ├─ attendance_views.sql
│  │  │  ├─ course_views.sql
│  │  │  └─ README.md
│  │  ├─ .gitkeep
│  │  └─ README.md
│  ├─ schemas/
│  │  ├─ 01_create_schemas.sql
│  │  └─ README.md
│  ├─ setup/
│  │  ├─ create_schemas.sql
│  │  └─ README.md
│  ├─ silver/
│  │  ├─ api/
│  │  │  ├─ attendance_data.sql
│  │  │  ├─ batches_data.sql
│  │  │  ├─ course_batch_merge.sql
│  │  │  ├─ course_catalogue.sql
│  │  │  ├─ course_metadeata.sql
│  │  │  └─ README.md
│  │  |
│  │  ├─ webhook/
│  │  │  ├─ announcements.sql
│  │  │  ├─ assessments.sql
│  │  │  ├─ certificates.sql
│  │  │  ├─ README.md
│  │  │  ├─ sessions.sql
│  │  │  ├─ transactions.sql
|  |  |  ├─ courses.sql
│  │  │  └─ users.sql
│  │  └─ README.md
│  ├─ verification/
│  │  ├─ check_data_quality.sql
│  │  ├─ check_row_counts.sql
│  │  └─ README.md
│  ├─ README.md
│  ├─ run_all.py
│  └─ run_all.sql
├─ docs/
│  ├─ api_endpoints.md
│  ├─ architecture.md
│  ├─ data_dictionary.md
│  ├─ decisions.md
│  └─ runbook.md
├─ logs/
│  ├─ attendance_backfill.err
│  ├─ attendance_backfill.log.err
│  └─ README.md
├─ manual_data_load/
│  ├─ backfill_transactions.py
│  ├─ csv_backfill_transactions.py
│  ├─ csv_load_bronze.py
│  ├─ csv_load_course_bronze.py
│  ├─ csv_transform_course_silver.py
│  ├─ load_courses_csv.py
│  ├─ load_students_csv.py
│  ├─ README.md
│  └─ transform_courses_silver.py
├─ pdf/
│  ├─ Edmingle API_01.pdf
│  ├─ Edmingle API_02.pdf
│  ├─ README.md
│  └─ vyoma_executive_report.pdf
├─ tests/
│  ├─ check_db_counts.py
│  ├─ clear_test_data.py
│  ├─ README.md
│  ├─ run_analysis.py
│  ├─ test_all_events.py
│  ├─ test_db_unavailability.py
│  ├─ test_pipeline_e2e.py
│  └─ test_webhook_send.py
├─ Webhook_scripts/
│  ├─ README.md
│  ├─ reprocess_bronze.py
│  └─ webhook_receiver.py
├─ .env.example
├─ .gitignore
└─ README.md
```
---

## bronze.webhook_events

Every webhook event received from Edmingle is stored here first, unchanged. This table is never modified after insert (except the `routed_to_silver` flag). It is the permanent audit log of all incoming data.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1`, `2`, `3` |
| `event_id` | TEXT | Unique ID for this event, assigned by Edmingle or derived from event type + timestamp | `user.user_created-2024-03-08T04:30:00+00:00` |
| `event_type` | TEXT | The Edmingle event name | `user.user_created`, `session.session_created` |
| `raw_payload` | JSONB | The complete original webhook body as received, stored without any modification | `{"event": {...}, "payload": {...}}` |
| `received_at` | TIMESTAMPTZ | When this row was inserted, in IST | `2024-03-08 10:00:00+05:30` |
| `is_live_mode` | BOOLEAN | True if this is a real production event; false for test/sandbox events | `true` |
| `routed_to_silver` | BOOLEAN | Set to true once the event has been successfully written to the Silver tables | `false` (default), `true` (after Silver insert) |

---

## bronze.failed_events

Any webhook request that arrived at the server but could not be processed is logged here. This includes malformed JSON, missing required fields, and DB errors. It is the safety net — nothing is silently dropped.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1` |
| `received_at` | TIMESTAMPTZ | When the failed request arrived, in IST | `2024-03-08 10:01:00+05:30` |
| `failure_reason` | TEXT | A short description of why it failed | `JSON parse failed`, `Bronze insert failed: ...` |
| `raw_body` | TEXT | The first 10,000 characters of the raw request body | `{invalid json here}` |
| `content_type` | TEXT | The Content-Type header of the failed request | `application/json`, `text/plain` |

---

## bronze.studentexport_raw

A verbatim copy of the `studentexport.csv` file exported from Edmingle. Every column from the CSV is stored as TEXT to avoid any data loss during import. This was a one-time historical backfill. All 56,000+ rows from the CSV are here.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1` |
| `source_row` | INTEGER | 0-based row index from the pandas DataFrame (unique, used to prevent duplicate imports) | `0`, `1`, `2` |
| `row_number` | TEXT | Row number as it appeared in the CSV | `1` |
| `name` | TEXT | Student full name | `Ramesh Kumar` |
| `email` | TEXT | Student email address | `ramesh@example.com` |
| `registration_number` | TEXT | Edmingle registration number | `VYO-00123` |
| `contact_number_dial_code` | TEXT | Dial code for primary phone | `+91` |
| `contact_number` | TEXT | Primary phone number | `9876543210` |
| `alternate_contact_number_dial_code` | TEXT | Dial code for alternate phone | `+91` |
| `alternate_contact_number` | TEXT | Alternate phone number | `9123456789` |
| `date_of_birth` | TEXT | Date of birth as a string | `01/15/1990` |
| `parent_name` | TEXT | Parent or guardian name | `Suresh Kumar` |
| `parent_contact` | TEXT | Parent phone number | `9812345678` |
| `parent_email` | TEXT | Parent email address | `suresh@example.com` |
| `address` | TEXT | Student home address | `123 Main Street, Mumbai` |
| `city` | TEXT | City | `Mumbai` |
| `state` | TEXT | State | `Maharashtra` |
| `standard` | TEXT | Academic standard or grade | `Class 10` |
| `date_created` | TEXT | Account creation date as a string | `3/8/2024 10:30` |
| `username` | TEXT | Edmingle username | `ramesh123` |
| `gender` | TEXT | Gender | `Male` |
| `status` | TEXT | Account status | `active` |
| `username_1` | TEXT | Secondary username field (Edmingle quirk) | `ramesh_k` |
| `why_study_sanskrit` | TEXT | Custom field: reason for learning Sanskrit | `Interested in ancient texts` |
| `user_nice_name` | TEXT | Display name | `Ramesh` |
| `user_last_name` | TEXT | Last name | `Kumar` |
| `would_like_to_teach` | TEXT | Custom field | `Yes` |
| `teaching_experience` | TEXT | Custom field | `2 years` |
| `is_mainstream_education` | TEXT | Custom field | `Yes` |
| `objective` | TEXT | Custom field: learning objective | `Read Vedas` |
| `user_age` | TEXT | Age as entered by student | `34` |
| `persona` | TEXT | Custom field | `Learner` |
| `objective_package` | TEXT | Custom field | `Certificate` |
| `time_per_week_hours` | TEXT | Hours per week available for study | `5` |
| `age_` | TEXT | Duplicate age field (Edmingle exports two) | `34` |
| `facebook_profile_url` | TEXT | Facebook URL | `https://facebook.com/ramesh` |
| `instagram_profile_url` | TEXT | Instagram URL | `https://instagram.com/ramesh` |
| `pinterest_profile_url` | TEXT | Pinterest URL | *(usually null)* |
| `soundcloud_profile_url` | TEXT | SoundCloud URL | *(usually null)* |
| `tumblr_profile_url` | TEXT | Tumblr URL | *(usually null)* |
| `youtube_profile_url` | TEXT | YouTube URL | *(usually null)* |
| `wikipedia_url` | TEXT | Wikipedia URL | *(usually null)* |
| `twitter_username` | TEXT | Twitter/X username | `@ramesh_k` |
| `gst_number` | TEXT | GST number for billing | *(usually null)* |
| `myspace_profile_url` | TEXT | MySpace URL | *(usually null)* |
| `international_phone_number` | TEXT | International format phone | `+919876543210` |
| `website` | TEXT | Personal website | *(usually null)* |
| `educational_qualification` | TEXT | Highest education level | `B.Com` |
| `linkedin_profile_url` | TEXT | LinkedIn URL | *(usually null)* |
| `age_v2` | TEXT | Third age field (Edmingle exports multiple) | `34` |
| `gender_` | TEXT | Duplicate gender field | `Male` |
| `sanskrit_qualification` | TEXT | Custom field | `Beginner` |
| `areas_of_interest` | TEXT | Custom field | `Grammar, Literature` |
| `studying_sanskrit_currently` | TEXT | Custom field | `Yes` |
| `current_education_status` | TEXT | Custom field | `Working professional` |
| `country_name` | TEXT | Country | `India` |
| `loaded_at` | TIMESTAMPTZ | When this row was loaded into the database | `2024-04-29 14:00:00+05:30` |

---

## bronze.student_courses_enrolled_raw

A verbatim copy of the `studentCoursesEnrolled.csv` file from Edmingle. All timestamps are stored as TEXT here and converted to typed values in Silver. This was a one-time historical backfill.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1` |
| `source_row` | INTEGER | 0-based row index from the CSV (unique, prevents duplicate imports) | `0` |
| `user_id` | TEXT | Edmingle user ID (stored as text in CSV) | `12345` |
| `name` | TEXT | Student name | `Ramesh Kumar` |
| `email` | TEXT | Student email | `ramesh@example.com` |
| `class_id` | TEXT | Internal class/batch identifier | `121333` |
| `class_name` | TEXT | Name of the batch | `Batch A 2024` |
| `tutor_name` | TEXT | Name of the teacher | `Prof. Sharma` |
| `total_classes` | TEXT | Total number of scheduled classes | `48` |
| `present` | TEXT | Number of classes attended | `40` |
| `absent` | TEXT | Number of classes missed | `8` |
| `late` | TEXT | Number of late arrivals | `2` |
| `excused` | TEXT | Number of excused absences | `1` |
| `start_date` | TEXT | Enrollment start date as Unix timestamp string | `1709856600` |
| `end_date` | TEXT | Enrollment end date as Unix timestamp string | `1741392600` |
| `master_batch_id` | TEXT | Batch identifier | `1281` |
| `master_batch_name` | TEXT | Batch name | `Batch A 2024` |
| `classusers_start_date` | TEXT | When this student joined the class | `1709856600` |
| `classusers_end_date` | TEXT | When this student's access ends | `1741392600` |
| `batch_status` | TEXT | Status of the overall batch | `active` |
| `cu_status` | TEXT | Status of this student's enrollment | `active` |
| `cu_state` | TEXT | State of the enrollment | `enrolled` |
| `institution_bundle_id` | TEXT | Institution's internal course bundle ID | `363` |
| `archived_at` | TEXT | When the enrollment was archived, if applicable | *(usually null)* |
| `bundle_id` | TEXT | Edmingle course bundle ID | `12477` |
| `loaded_at` | TIMESTAMPTZ | When this row was loaded into the database | `2024-04-29 14:00:00+05:30` |

---

## bronze.unresolved_students_raw

Students from `studentexport.csv` whose email address could not be matched to any `user_id` in Edmingle's system at the time of the backfill. These 22,834 rows were not loaded into `silver.users` because without a `user_id` they cannot be joined to enrollment or attendance data.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1` |
| `source_row` | INTEGER | Row index from the original CSV | `42` |
| `email` | TEXT | The email address that could not be resolved | `unknown@example.com` |
| `raw_row` | JSONB | The complete original CSV row stored as JSON | `{"name": "...", "email": "...", ...}` |
| `inserted_at` | TIMESTAMPTZ | When this row was inserted | `2024-04-29 14:00:00+05:30` |

---

## bronze.course_lifecycle_tracker (Elearning_MIS_Merged_Tracker)
A master operational tracker maintained manually (likely in Google Sheets) that records the end-to-end lifecycle of every course run on the platform. Each row represents one course batch, from finalisation through post-production closeout. Contains ~1,055 rows and 107 columns.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `Sl No` | INTEGER | Sequential row number in the tracker | `1`, `2`, `3` |
| `Course Name` | TEXT | Full name of the course | `Learn to chant Sriman Narayaneeyam` |
| `Course ID` | INTEGER | Edmingle bundle/course ID | `6495` |
| `Batch Name` | TEXT | Name of the specific batch run | `Learn to chant Sriman Narayaneeyam` |
| `Type of Launch` | TEXT | How the course was launched | `Past Webinar`, `Upcoming Webinar` |
| `Status` | TEXT | Current lifecycle status of the course | `Completed`, `Ongoing`, `Upcoming` |
| `Subject` | TEXT | Subject area of the course | `Stotra - Parayanam (Chanting)` |
| `Position in Funnel` | TEXT | Marketing funnel position | `Top`, `Middle`, `Bottom` |
| `Samskritadhyayana Model` | TEXT | Internal pedagogical classification | `Bhashadhyayanam`, `Vedadhyayanam` |
| `Term of Course` | TEXT | Duration category | `Long`, `Short`, `Mid` |
| `SSS Category` | TEXT | Internal content category | `SSS`, `Samskrta` |
| `Persona` | TEXT | Comma-separated list of target student personas | `Professionals , Homemakers , Senior Citizens` |
| `batch_id` | FLOAT | Edmingle batch ID (sparse — not always filled) | `1281` |
| `New_1` | FLOAT | Unlabelled auxiliary column | *(usually null)* |
| `New_2` | FLOAT | Unlabelled auxiliary column | *(usually null)* |
| `No. of classes in a week` | INTEGER | Frequency of live sessions per week | `1`, `2` |
| `Days of Classes` | TEXT | Which days of the week classes are held | `Saturday`, `Monday, Thursday` |
| `Class Timings IST` | TEXT | Live session time window in IST | `8:00 PM to 9:00 PM` |
| `Webinar Tool` | TEXT | Platform used for live sessions | `Zoom Webinar 1`, `Zoom Meeting` |
| `Course Finalisation date` | TEXT | Date when course content was locked | `5/10/2023` |
| `Course Launch Date` | TEXT | Date the course was made public | `3/15/2023`, `Unavailable` |
| `First Class Date` | TEXT | Date of the first live session | `4/4/2020` |
| `Last Class and Valedictory Date` | TEXT | Date of the final class | `8/12/2023` |
| `Primary Teacher (Name + sfh Link)` | TEXT | Name and profile link of the lead teacher | `Shri. (Dr.) Shankararama Sharma` |
| `Primary Teacher ID` | INTEGER | Edmingle user ID of the primary teacher | `18984715` |
| `Additional Teacher (Name + Link)` | FLOAT | Name and link of a supporting teacher, if any | *(usually null)* |
| `Additional Teacher ID` | FLOAT | Edmingle user ID of the additional teacher | *(usually null)* |
| `ELA` | FLOAT | Name of the E-Learning Associate assigned | *(usually null)* |
| `Employee ID` | TEXT | Internal employee ID of the ELA | *(usually null)* |
| `Panelists` | FLOAT | Panelists for the course, if applicable | *(usually null)* |
| `Volunteer for class upload` | TEXT | Name of volunteer handling video uploads | `Priya` |
| `Volunteer for Timestamping` | TEXT | Name of volunteer handling YouTube timestamps | `Sharmila` |
| `Enrolments on next day of launch` | TEXT | Enrollment count one day after course went live | `450`, `Unavailable` |
| `Enrolments on the day of first class` | FLOAT | Enrollment count on the day of first session | `620.0` |
| `First Class Attendance` | INTEGER | Number of students who attended the first class | `700` |
| `Second Class Attendance` | FLOAT | Number of students who attended the second class | `580.0` |
| `Enrolments on last day` | TEXT | Enrollment count on the last class day | `3,304` |
| `Last Class Attendance` | INTEGER | Number of students who attended the final class | `259` |
| `Total no of classes held` | FLOAT | Total number of live sessions conducted | `215.0` |
| `Total Hours of classes` | INTEGER | Total hours of instruction delivered | `215` |
| `Average Attendance of all classes` | FLOAT | Mean attendance across all sessions | `169.96` |
| `Average Attendance in last 5 classes` | FLOAT | Mean attendance in the final 5 sessions | `199.66` |
| `Enrollment as on 31 Mar 2025` | FLOAT | Point-in-time enrollment snapshot | *(sparse)* |
| `Enrollment as on 31 Aug 2025` | FLOAT | Point-in-time enrollment snapshot | *(sparse)* |
| `Type of Assessment` | TEXT | Format of the end-of-course assessment | `Oral`, `Written`, `Online` |
| `Date of Assessment Announcement mail sent` | TEXT | When students were notified of the assessment | `8/5/2023` |
| `Assessment Start Date` | TEXT | First date students could take the assessment | `8/26/2023` |
| `Assessment End Date` | TEXT | Last date students could take the assessment | `8/29/2023` |
| `Total assessment Attendees` | INTEGER | Number of students who appeared for assessment | `225` |
| `Judges for Oral Assessment` | FLOAT | Names of judges for oral assessments | *(usually null)* |
| `Results announcement date` | TEXT | When results were published | `9/10/2023` |
| `Total Students certified` | INTEGER | Number of students who passed and received certificates | `186` |
| `Date of Certificates issued` | TEXT | When certificates were sent out | `9/20/2023` |
| `Time given for course finaliastion and launch` | Days between finalisation and launch | At least 3–5 days |
| `Time given for launch & first class` | Days between launch and first session | At least 15 days |
| `Time given btw last class and feedack` | Days between last class and feedback request | Should not exceed 1 day |
| `Time given btw Last Class & Assessment Announcement` | Days between last class and assessment notice | Should not exceed 3 days |
| `Time given btw Assessment Announcement & Start date` | Days between announcement and assessment start | At least 15 days |
| `Time gap given for Conducting Assessment & Teacher Satisfaction survey` | Days between assessment end and teacher survey | Should not exceed 7 days |
| `Time gap given for Completion of Assessment & Results Announcement` | Days to announce results after assessment | Should not exceed 7 days |
| `Time gap given for Results Announcement & Certificate issue` | Days between results and certificates | Should not exceed 15 days |
| `Time given for Certificate issued & Close out Email date` | Days between certificates and close-out email | Should not exceed 3 days |
| `Total Certified vs Initial Enrolments` | Certification rate vs. initial sign-ups |
| `Total Certified vs Course-end enrolments` | Certification rate vs. final enrollment count |
| `Total Certified vs First class Attendees` | Certification rate vs. first-class attendees |
| `Total Certified vs Average Attendees` | Certifications relative to average live attendance |
| `First Class attendance vs Initial enrolments` | Drop-off between enrolment and first class |
| `First Class attendance vs Last Class attendance` | Attendance retention over the course duration |
| `Pass Percentage (Total certified vs Total assessment attendees)` | Of those who took the exam, percentage who passed |
| `Pass Percentage (Total students on last class vs Total certified)` | Certified students relative to last-class attendance |
| `Date of Request for course Feedback to students` | TEXT | When the student feedback form was sent | `8/10/2023` |
| `Date of Close out Email sent` | TEXT | When the final closure email was sent to students | `9/25/2023` |
| `Total Number of Feedbacks received` | FLOAT | Count of feedback form submissions | `142.0` |
| `Overall course rating` | FLOAT | Aggregate student rating out of 5 | `5.0` |
| `Average Rating for Ease of attending live webinars` | FLOAT | Student rating for webinar accessibility | `4.23` |
| `Average Rating for Quality of content and course materials` | FLOAT | Student rating for content quality | `4.49` |
| `Average Teacher Rating` | FLOAT | Student rating for the teacher | `4.56` |
| `Average Rating for Accessing course materials in the website` | FLOAT | Student rating for website usability | `4.47` |
| `Average ELA Rating by teacher` | FLOAT | Teacher's rating of the ELA support | *(usually null)* |
| `Average Content Support Rating by teacher` | FLOAT | Teacher's rating of content support | *(usually null)* |
| `Overall course Rating by teacher` | FLOAT | Teacher's overall course rating | *(usually null)* |
| `Hours worked by Teacher for the course` | FLOAT | Total hours logged by the primary teacher |
| `Hours worked by ELA for the course` | FLOAT | Total hours logged by the ELA |
| `Hours worked by Linguist Team for the course` | FLOAT | Total hours logged by linguist team |
| `Average hours worked by Volunteers` | FLOAT | Average volunteer hours per course |
| `Average hours worked by Judges for oral assessment` | FLOAT | Average judge hours for oral assessments |
| `Drive Folder` | TEXT | Name/link of the Google Drive folder for this course |
| `YouTube Playlist Name and link` | TEXT | YouTube playlist for course recordings |
| `Clickup folder link` | TEXT | ClickUp task folder link for project management |
| `Canva - Banner link` | TEXT | Canva link for the course banner |
| `Feedback Form` | TEXT | Google Form link for student feedback |
| `Feedback Form Responses sheet` | TEXT | Google Sheets link for feedback responses |
| `Course Master` | FLOAT | Reference to course master document *(sparse)* |
| `Attendance sheet download status` | Whether attendance data was downloaded |
| `Status for filling in CSV sheet for Testimonials` | Testimonial CSV population status |
| `Status for adding Testimonials on SFH` | Whether testimonials were published on the website |
| `Playlist description added` | YouTube playlist description added |
| `Video Title Standardised` | Whether video titles follow naming standards |
| `Video Description standardised` | Whether video descriptions are standardized |
| `Thumbnail images uploaded` | Whether thumbnails are uploaded to YouTube |
| `Tags added` | Whether tags are added to YouTube videos |
| `Monetization (Everything except during video)` | Monetization settings configured |
| `Timestamping done` | Whether video timestamps are added |
| `Starting and Ending video bits added` | Intro/outro clips added to videos |
| `Endscreen links updated` | YouTube endscreen links configured |
| `Cards updated` | YouTube cards updated |
 
---
 
## bronze.batches_data
A clean, structured export of all batch-level records from Edmingle. Each row represents one batch (class group) linked to a course bundle. Contains ~845 rows and 12 columns. This is the closest thing to a normalized batch dimension table available in the source exports.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `bundle_id` | INTEGER | Edmingle course bundle ID. Foreign key to `course_catalogue_data.Bundle id` and `bronze.student_courses_enrolled_raw.bundle_id` | `6340` |
| `bundle_name` | TEXT | Name of the parent course bundle | `Siddhanta Kaumudi - Atmanepada Prakaranam` |
| `batch_id` | INTEGER | Edmingle batch (class) ID. Foreign key to `bronze.student_courses_enrolled_raw.class_id` | `12443` |
| `batch_name` | TEXT | Name of this specific batch | `Siddhanta Kaumudi - Atmanepada Prakaranam` |
| `batch_status` | TEXT | Current status of the batch | `Active`, `Inactive`, `Completed` |
| `start_date` | INTEGER | Batch start date as a Unix timestamp (seconds since epoch) | `1390435200` |
| `start_date_converted` | TEXT | Human-readable start date in IST | `23-01-2014 05:30 AM IST` |
| `end_date` | INTEGER | Batch end date as a Unix timestamp | `1392336000` |
| `end_date_converted` | TEXT | Human-readable end date in IST | `14-02-2014 05:30 AM IST` |
| `tutor_id` | INTEGER | Edmingle user ID of the assigned tutor. Foreign key to `bronze.webhook_events` teacher records | `18983595` |
| `tutor_name` | TEXT | Name of the assigned tutor | `Dr. Venkatasubramanian P` |
| `admitted_students` | INTEGER | Number of students admitted to this batch | `385` |
 
---
 
## bronze.course_catalogue
A full export of the course catalogue from the Vyoma/Sanskritfromhome platform. Each row represents one course (bundle) as it appears on the public-facing website. Contains 59 columns. Includes rich HTML content fields (overviews, descriptions) alongside structured

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `Bundle id` | INTEGER | Edmingle course bundle ID. Primary key for this file. Foreign key to `batches_data.bundle_id` | `27432` |
| `Course Name` | TEXT | Full display name of the course | `Samskritasri Pathamala 4` |
| `Course Title Sanskrit` | TEXT | Course name in Devanagari script | `संस्कृतश्रीः पाठमाला - ४` |
| `Pretty Name` | TEXT | URL slug used in the course page URL | `learn-basic-sanskrit-pathamala4` |
| `Course URL` | TEXT | Full public URL of the course on sanskritfromhome.org | `https://www.sanskritfromhome.org/...` |
| `Course List` | TEXT | Display name in course listing pages | `Samskritasri Pathamala 4` |
| `Course Ids` | INTEGER | Internal course (class) IDs linked to this bundle | `45359` |
| `Status` | TEXT | Publication status of the course | `Upcoming`, `Active`, `Completed` |
| `Coming soon` | OBJECT | Whether the course is flagged as coming soon | `True`, `False`, `nan` |
| `Whats new` | BOOLEAN | Whether this course is marked as new | `True`, `False` |
| `Whats new poster` | TEXT | Promotional poster text for new courses | *(usually blank)* |
| `Hide in Ongoing Webinar` | FLOAT | Flag to suppress from ongoing webinar listings | *(sparse)* |
| `Course Ordering` | FLOAT | Manual ordering weight for display | *(sparse)* |
| `Subject` | TEXT | Subject area of the course | `Language Practice`, `Vedic Chanting` |
| `Level` | TEXT | Difficulty level | `Beginner`, `Intermediate`, `Advanced` |
| `Language` | TEXT | Medium of instruction | `English`, `Hindi`, `Tamil` |
| `Type` | TEXT | Delivery format | `Live Webinars`, `Self-Paced` |
| `Course Division` | TEXT | Top-level content division | `Course`, `Learning Program` |
| `Division` | TEXT | Sub-division classification | *(sparse)* |
| `SSS Category` | TEXT | Internal SSS content category | `Samskrta`, `Veda`, `Shastra` |
| `Adhyayanam Category` | TEXT | Internal learning model category | `Bhashadhyayanam`, `Vedadhyayanam` |
| `Term of Course` | TEXT | Duration classification | `Short`, `Mid`, `Long` |
| `Position in Funnel` | TEXT | Marketing funnel position | `Top`, `Middle`, `Lower Middle` |
| `Examination` | FLOAT | Whether the course has an examination | *(sparse)* |
| `Texts` | FLOAT | Primary texts studied in the course | *(sparse)* |
| `Certificate` | OBJECT | Whether a certificate is awarded on completion | `True`, `False` |
| `Target Audience` | TEXT | Intended audience for the course | `Students ,Age 15+` |
| `Personas` | TEXT | Comma-separated list of target personas | `University Students,Professionals ,Homemakers` |
| `Cost` | INTEGER | Course fee in INR (0 = free) | `0`, `500` |
| `Is Online Package` | INTEGER | Whether the course is an online package (1 = yes) | `1` |
| `Online Registration Allowed` | INTEGER | Whether students can self-register online (1 = yes) | `1` |
| `Free Preview Allowed` | INTEGER | Whether a free preview is available (1 = yes) | `0`, `1` |
| `Num Students` | INTEGER | Total number of enrolled students | `130`, `4500` |
| `Post Enrollment (Redirect URL)` | FLOAT | URL to redirect students to after enrollment | *(sparse)* |
| `Tutors` | TEXT | Name(s) of the teacher(s) for this course | `Smt. Sriranjani Vijaykumar` |
| `Tutord Ids` | INTEGER | Edmingle user ID(s) of the teacher(s) | `89807447` |
| `Number of Lectures` | INTEGER | Total planned number of sessions | `35` |
| `Duration` | TEXT | Duration expressed as number of classes or time | `35`, `6 months` |
| `Duration - old` | FLOAT | Legacy duration field | *(sparse)* |
| `Live session Schedule text` | TEXT | Free-text description of the live session schedule | `Start Date: 4th February 2026; Days: Every Wednesday & Friday; Time: 11:00 AM - 12:00 PM (IST)` |
| `Course Description` | TEXT | Short course description paragraph |
| `Overview` | TEXT | Full HTML course overview including learning outcomes, prerequisites, materials |
| `About The course` | FLOAT | Legacy about section *(sparse)* |
| `Know more about the course` | TEXT | Supplementary HTML content including sponsor details, about-the-teacher sections, FAQs |
| `About this Learning Program` | FLOAT | For learning programs: program overview *(sparse)* |
| `Learning Program Value Proposi` | FLOAT | For learning programs: value proposition *(sparse)* |
| `How Learning Program Works` | FLOAT | For learning programs: operational details *(sparse)* |
| `Know More About The Programs` | FLOAT | For learning programs: extended detail *(sparse)* |
| `Ongoing Webinar Note` | FLOAT | Note shown when course is live/ongoing *(sparse)* |
| `Eligibility` | FLOAT | Eligibility criteria (sometimes embedded in Overview instead) *(sparse)* |
| `Course Sponsor` | TEXT | Sponsor name or CTA text for the course | *(usually blank or `Sponsor this course`)* |
| `Meta Title` | FLOAT | SEO page title *(sparse)* |
| `Meta Description` | FLOAT | SEO meta description *(sparse)* |
| `Meta Keywords` | FLOAT | SEO keywords *(sparse)* |
| `dsg link` | FLOAT | DSG (design/graphics) link *(sparse)* |
| `Computer Based Assessment` | FLOAT | Whether course has a CBA *(sparse)* |
| `Credits` | FLOAT | Academic credits awarded *(sparse)* |
| `Product ID` | FLOAT | External product ID if applicable *(sparse)* |
| `Viniyoga` | OBJECT | Traditional usage/application classification *(sparse)* |
 


## silver.users

One row per student. When Edmingle sends a `user.user_created` or `user.user_updated` event, this table is upserted — if the student already exists, their fields are updated using COALESCE (existing non-null values are never overwritten by null).

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1` |
| `event_id` | TEXT | The event_id from the most recent webhook that touched this row | `user.user_updated-2024-03-08T05:00:00+00:00` |
| `event_type` | TEXT | The event type that last updated this row | `user.user_updated` |
| `user_id` | BIGINT | Edmingle's unique ID for this student (upsert key) | `123456` |
| `email` | TEXT | Student email address | `ramesh@example.com` |
| `full_name` | TEXT | Student full name | `Ramesh Kumar` |
| `user_name` | TEXT | Edmingle username | `ramesh123` |
| `user_role` | TEXT | Role in the platform | `student` |
| `contact_number` | TEXT | Phone number | `9876543210` |
| `institution_id` | INTEGER | Vyoma's institution ID in Edmingle (483 for Vyoma) | `483` |
| `city` | TEXT | City (from system_fields in user_updated events) | `Mumbai` |
| `state` | TEXT | State | `Maharashtra` |
| `address` | TEXT | Full address | `123 Main Street` |
| `pincode` | TEXT | PIN code | `400001` |
| `parent_name` | TEXT | Parent name (from system_fields) | `Suresh Kumar` |
| `parent_email` | TEXT | Parent email (from system_fields) | `suresh@example.com` |
| `parent_contact` | TEXT | Parent phone (from system_fields) | `9812345678` |
| `custom_fields` | JSONB | Any custom fields sent in the event, stored as JSON | `{"standard": "Class 10"}` |
| `created_at_ist` | TIMESTAMPTZ | Account creation time in IST; set once, never overwritten | `2024-03-08 10:00:00+05:30` |
| `updated_at_ist` | TIMESTAMPTZ | Last profile update time in IST | `2024-03-09 11:00:00+05:30` |
| `received_at` | TIMESTAMPTZ | When this row was last written to the database, in IST | `2024-03-09 11:00:05+05:30` |

---

## silver.transactions

One row per student-course-batch enrollment. When a student buys the same course in a different batch, that is a separate row. CSV backfill data and live webhook data for the same enrollment merge into a single row (upsert on `user_id + bundle_id + master_batch_id`).

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1` |
| `event_id` | TEXT | Event ID from the most recent webhook that touched this row | `transaction.user_purchase_completed-2024-03-08T04:30:00+00:00` |
| `event_type` | TEXT | Event type | `transaction.user_purchase_completed` |
| `event_timestamp_ist` | TIMESTAMPTZ | When the event occurred, in IST | `2024-03-08 10:00:00+05:30` |
| `user_id` | BIGINT | Student's Edmingle user ID (part of upsert key) | `123456` |
| `email` | TEXT | Student email at time of purchase | `ramesh@example.com` |
| `full_name` | TEXT | Student name at time of purchase | `Ramesh Kumar` |
| `contact_number` | TEXT | Phone number | `9876543210` |
| `bundle_id` | BIGINT | Edmingle course bundle ID (part of upsert key) | `12477` |
| `course_name` | TEXT | Name of the course | `Sanskrit Foundation Course` |
| `master_batch_id` | BIGINT | Batch ID within the course (part of upsert key) | `1281` |
| `master_batch_name` | TEXT | Batch name | `Batch A 2024` |
| `institution_bundle_id` | BIGINT | Vyoma's internal bundle ID | `363` |
| `original_price` | NUMERIC(12,2) | Listed price before discount | `5000.00` |
| `discount` | NUMERIC(12,2) | Discount amount applied | `500.00` |
| `final_price` | NUMERIC(12,2) | Amount actually paid | `4500.00` |
| `currency` | TEXT | Currency code | `INR` |
| `credits_applied` | NUMERIC(12,2) | Platform credits used | `0.00` |
| `payment_method` | TEXT | How the student paid | `razorpay`, `upi` |
| `transaction_id` | TEXT | Payment gateway transaction reference | `pay_abc123` |
| `start_date_ist` | TIMESTAMPTZ | Enrollment start date in IST | `2024-03-08 10:00:00+05:30` |
| `end_date_ist` | TIMESTAMPTZ | Enrollment end date in IST | `2025-03-08 10:00:00+05:30` |
| `created_at_ist` | TIMESTAMPTZ | When the enrollment was created in IST | `2024-03-08 09:55:00+05:30` |
| `source` | TEXT | Where this row came from | `webhook`, `csv` |
| `inserted_at` | TIMESTAMPTZ | When this row was last written to the database | `2024-03-08 10:00:05+05:30` |

---

## silver.sessions

One row per live class session. Multiple webhook events can refer to the same session (created, updated, started, cancelled) — they all upsert onto the same row using `attendance_id` as the key.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1` |
| `event_id` | TEXT | Event ID from the most recent webhook that touched this row | `session.session_started-2024-03-08T04:45:00+00:00` |
| `event_type` | TEXT | The event type that last updated this row | `session.session_started` |
| `attendance_id` | BIGINT | Edmingle's unique ID for this session instance (upsert key) | `8914546` |
| `class_id` | BIGINT | The recurring class this session belongs to | `121333` |
| `class_name` | TEXT | Human-readable class name | `Sanskrit Grammar — Lesson 1` |
| `class_type_formatted` | TEXT | Type of class | `Live Class`, `Recorded` |
| `scheduled_start_ist` | TIMESTAMPTZ | Planned start time in IST | `2024-03-08 10:00:00+05:30` |
| `scheduled_end_ist` | TIMESTAMPTZ | Planned end time in IST | `2024-03-08 11:00:00+05:30` |
| `actual_start_ist` | TIMESTAMPTZ | When the session actually started (from session_started event) | `2024-03-08 10:05:00+05:30` |
| `duration_minutes` | INTEGER | Planned duration in minutes | `60` |
| `teacher_id` | BIGINT | Edmingle user ID of the teacher | `15` |
| `teacher_name` | TEXT | Teacher's name | `Prof. Ramesh Sharma` |
| `teacher_email` | TEXT | Teacher's email | `ramesh@example.com` |
| `master_batches` | JSONB | List of batches attending this session | `[{"master_batch_id": 1281, ...}]` |
| `schedule_id` | BIGINT | ID of the schedule this session belongs to | `5001` |
| `is_recurring` | BOOLEAN | Whether this is part of a recurring schedule | `true` |
| `virtual_platform` | TEXT | Online class platform | `Zoom`, `GoogleMeet` |
| `zoom_meeting_id` | TEXT | Zoom meeting ID if applicable | `123456789` |
| `cancellation_reason` | TEXT | Why the session was cancelled (from session_cancel event) | `Teacher unavailable` |
| `cancelled_by` | BIGINT | User ID of whoever cancelled it | `15` |
| `status` | INTEGER | Session status code from Edmingle | `0` (scheduled), `1` (live), `2` (ended) |
| `is_late_signin` | BOOLEAN | True if the teacher signed in late | `true` |
| `delay_minutes` | INTEGER | How many minutes late the session started | `5` |
| `reminder_type` | TEXT | Type of reminder sent (from session_reminders event) | `1h_before` |
| `received_at` | TIMESTAMPTZ | When this row was last written in IST | `2024-03-08 10:05:10+05:30` |

---

## silver.assessments

One row per assessment event. There are four event types: `test_submitted`, `test_evaluated`, `exercise_submitted`, `exercise_evaluated`. Each has a unique `event_id`, so each produces its own row.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1` |
| `event_id` | TEXT | Unique event ID (upsert key) | `assess-test-sub-001` |
| `event_type` | TEXT | Which assessment event this is | `assessments.test_submitted` |
| `user_id` | BIGINT | Student who submitted | `123456` |
| `attempt_id` | BIGINT | Edmingle ID for this attempt | `55001` |
| `exercise_id` | BIGINT | ID of the exercise (null for test events, which use attempt_id only) | `7001` |
| `mark` | NUMERIC(8,2) | Score awarded | `85.50` |
| `is_evaluated` | INTEGER | 0 = not yet evaluated, 1 = evaluated by faculty | `1` |
| `faculty_comments` | TEXT | Feedback from the teacher (null until evaluated event arrives) | `Good work. Review chapter 3.` |
| `submitted_at_ist` | TIMESTAMPTZ | When the student submitted, in IST | `2024-03-08 11:00:00+05:30` |
| `received_at` | TIMESTAMPTZ | When this row was last written in IST | `2024-03-08 11:00:10+05:30` |

---

## silver.courses

One row per course completion event. A student completing a course triggers a `course.user_course_completed` event.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1` |
| `event_id` | TEXT | Unique event ID (upsert key) | `course-completed-001` |
| `event_type` | TEXT | Always `course.user_course_completed` | `course.user_course_completed` |
| `user_id` | BIGINT | Student who completed the course | `123456` |
| `bundle_id` | BIGINT | The course that was completed | `12477` |
| `completed_at_ist` | TIMESTAMPTZ | When the course was completed, in IST | `2025-03-08 10:00:00+05:30` |
| `received_at` | TIMESTAMPTZ | When this row was last written in IST | `2025-03-08 10:00:05+05:30` |

---

## silver.announcements

One row per announcement. The announcement payload structure from Edmingle has not yet been fully documented, so the entire data object is stored as JSONB for now.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1` |
| `event_id` | TEXT | Unique event ID (upsert key) | `announce-001` |
| `event_type` | TEXT | Always `announcement.announcement_created` | `announcement.announcement_created` |
| `raw_data` | JSONB | Complete data payload from the announcement event | `{"title": "Maintenance", "message": "..."}` |
| `received_at` | TIMESTAMPTZ | When this row was last written in IST | `2024-03-08 10:00:05+05:30` |

---

## silver.certificates

One row per certificate issued to a student.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1` |
| `event_id` | TEXT | Unique event ID (upsert key) | `cert-001` |
| `event_type` | TEXT | Always `certificate.certificate_issued` | `certificate.certificate_issued` |
| `certificate_id` | TEXT | Edmingle's certificate reference number | `CERT-2024-123456-12477` |
| `user_id` | BIGINT | Student who received the certificate | `123456` |
| `issued_at_ist` | TIMESTAMPTZ | When the certificate was issued, in IST | `2025-03-08 10:00:00+05:30` |
| `received_at` | TIMESTAMPTZ | When this row was last written in IST | `2025-03-08 10:00:05+05:30` |

---
## silver.course_batch_merge
A derived, analysis-ready table joining batch-level operational data with course catalogue metadata. Each row represents one batch enriched with its parent bundle's classification and status logic. 843 rows, 34 columns, one row per `batch_id`.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `bundle_id` | INTEGER | Edmingle course bundle ID. Foreign key to `course_catalogue_data` and `batches_data` | `6339` |
| `bundle_name` | TEXT | Name of the parent course bundle | `Siddhanta-Kaumudi - Atmanepada Prakaranam` |
| `batch_id` | INTEGER | Edmingle batch ID. Foreign key to `batches_data.batch_id` and `bronze.student_courses_enrolled_raw.class_id` | `12442` |
| `batch_name` | TEXT | Name of this specific batch run | `Siddhanta-Kaumudi - Atmanepada Prakaranam` |
| `Course Ids` | FLOAT | Edmingle internal class/session IDs linked to this bundle | `9970` |
| `Final_Status` | TEXT | Authoritative status for reporting. Derived by reconciling `batch_status`, `Catalogue_Status`, and any manual override. Use this over `Status` or `batch_status` for funnel and count metrics. | `Completed`, `Ongoing`, `Upcoming` |
| `Catalogue_Status` | TEXT | Resolved status from the course catalogue side of the join. May differ from `batch_status` when Edmingle and the catalogue are out of sync. | `Completed`, `Ongoing`, `Upcoming` |
| `Status` | TEXT | Raw status field from the course catalogue listing | `Completed`, `Ongoing`, `Upcoming` |
| `batch_status` | TEXT | Raw batch status as returned by Edmingle | `Archived`, `Active`, `Completed` |
| `Status_Adjustment_Reason` | TEXT | Free-text reason for any manual override of `Final_Status`. Null when no adjustment was made. | *(always null in current data)* |
| `Is_Latest_Batch` | INTEGER | `1` if this is the most recent batch for its parent bundle. Used to deduplicate when aggregating at bundle level. | `1`, `0` |
| `Include_In_Course_Count` | INTEGER | `1` if this batch should be included in official course-count reporting. Excludes test runs, duplicates, or inactive shells. | `1`, `0` |
| `Has_Batch` | INTEGER | `1` if this bundle has at least one associated batch record. Always `1` in this file — batches with no bundle are excluded at source. | `1` |
| `start_date` | DATE | Batch start date in `YYYY-MM-DD` format. Already converted from Unix timestamp. | `2014-01-23` |
| `end_date` | DATE | Batch end date in `YYYY-MM-DD` format. Already converted from Unix timestamp. | `2014-02-14` |
| `batch_enrollment_count` | FLOAT | Number of students enrolled in this specific batch | `385` |
| `bundle_enrollment_count` | FLOAT | Total enrollment across all batches for the parent bundle. Sourced from `course_catalogue_data.Num Students` | `1240` |
| `tutor_name` | TEXT | Name of the assigned tutor. `Team_Vyoma` is used as a placeholder when no specific tutor is assigned. | `Dr. Venkatasubramanian P`, `Team_Vyoma` |
| `Course Division` | TEXT | Top-level content division | `Course`, `Learning Path`, `Learning Program` |
| `Type` | TEXT | Delivery format | `Live Webinars`, `Past Webinars`, `Pre-recorded Video`, `Pre-recorded Audio`, `Discourse`, `E-text` |
| `Subject` | TEXT | Subject area of the course | `Vyakarana Shastra`, `Vedic Chanting` |
| `Level` | TEXT | Difficulty or proficiency level | `Basic`, `Intermediate`, `Shastra - Basic`, `Shastra - Advanced` |
| `Language` | TEXT | Medium of instruction | `Sanskrit`, `English`, `Hindi`, `Tamil` |
| `SSS Category` | TEXT | Internal SSS content category. Can be comma-separated for cross-category courses. | `Samskrta`, `Samskrti`, `Samskara` |
| `Adhyayanam Category` | TEXT | Internal learning model category | `Bhashadhyayanam`, `Shastradhyayanam`, `Granthadhyayanam`, `Viniyoga` |
| `Personas` | TEXT | Comma-separated list of target student personas | `Professionals ,Homemakers,Senior Citizens` |
| `Position in Funnel` | TEXT | Marketing funnel position | `Top`, `Middle`, `Lower Middle`, `Bottom` |
| `Term of Course` | TEXT | Duration category | `Very Short`, `Short`, `Mid`, `Long` |
| `Texts` | TEXT | Primary Sanskrit texts studied in this course | `Siddhanta Kaumudi`, `Amarakosha` |
| `Certificate` | BOOLEAN | Whether a certificate is awarded on completion | `True`, `False` |
| `Number of Lectures` | FLOAT | Total planned number of sessions | `9`, `48` |
| `Duration` | FLOAT | Course duration expressed as number of sessions or weeks | `8`, `24` |
| `Computer Based Assessment` | FLOAT | Whether the course has a computer-based assessment | *(always null in current data)* |
| `Course Sponsor` | TEXT | Sponsoring organisation or individual, if any | *(usually null)* |
 
---

## silver.course_metadata

This is the "course_metadata" course table that joins everything together. It contains 65 columns covering course details, schedules, attendance, and performance metrics. Power BI reads directly from this table.

| Column | Type | Description |
| :--- | :--- | :--- |
| `bundle_id` | BIGINT | Unique ID for the course bundle from Edmingle. |
| `course_name` | TEXT | Official name of the course. |
| `batch_id` | BIGINT | Unique ID for the specific batch from Edmingle. |
| `start_date` | TIMESTAMPTZ | Scheduled start date of the batch. |
| `end_date` | TIMESTAMPTZ | Scheduled end date of the batch. |
| `admitted_students` | INTEGER | Number of students admitted (from MIS tracker). |
| `num_students` | INTEGER | Current number of students enrolled in Edmingle. |
| `tutors` | TEXT | Names of the teachers. |
| `tutor_ids` | TEXT | ID numbers of the teachers. |
| `course_ids` | TEXT | IDs of the sub-courses in this bundle. |
| `subject` | TEXT | The main subject (e.g., Sanskrit). |
| `level` | TEXT | Difficulty level (Beginner, Advanced). |
| `language` | TEXT | Language of instruction. |
| `texts` | TEXT | Study materials used. |
| `type` | TEXT | Course type (Live, Recorded). |
| `course_division` | TEXT | Internal division name. |
| `certificate` | TEXT | Whether a certificate is issued. |
| `course_sponsor` | TEXT | Name of the sponsor. |
| `status` | TEXT | Current status (Active, Completed). |
| `number_of_lectures` | TEXT | Total planned lectures. |
| `duration` | TEXT | Course duration in months/weeks. |
| `personas` | TEXT | Target audience groups. |
| `sss_category` | TEXT | SSS internal classification. |
| `adhyayanam_category` | TEXT | Adhyayanam internal classification. |
| `term_of_course` | TEXT | The academic term. |
| `position_in_funnel` | TEXT | Stage in the marketing funnel. |
| `classes_per_week` | TEXT | Number of classes held each week. |
| `class_days` | TEXT | Days of the week classes are held. |
| `class_timings` | TEXT | Class timing in IST. |
| `additional_teacher` | TEXT | Names and links for other teachers. |
| `ela` | TEXT | Early Learning Assessment status. |
| `employee_id` | TEXT | ID of the employee managing the course. |
| `panelists` | TEXT | List of panelists for sessions. |
| `launch_date` | DATE | Official course launch date. |
| `enrollments_after_launch` | INTEGER | Enrollment count one day after launch. |
| `first_class_date` | DATE | Date of the very first class. |
| `enrollments_on_first_class` | INTEGER | Enrollments on the first class day. |
| `first_class_attendance` | INTEGER | Attendance count for first class. |
| `second_class_attendance` | INTEGER | Attendance count for second class. |
| `last_class_date` | DATE | Date of the last/valedictory class. |
| `enrollments_on_last_day` | INTEGER | Enrollments on the final class day. |
| `last_class_attendance` | INTEGER | Attendance count for last class. |
| `total_classes_held` | INTEGER | Actual number of sessions conducted. |
| `total_class_hours` | NUMERIC | Total teaching hours. |
| `avg_attendance` | NUMERIC | Average attendance across all classes. |
| `assessment_type` | TEXT | Type of assessment (Exam, Quiz). |
| `assessment_start_date` | DATE | Start date of final exams. |
| `assessment_end_date` | DATE | End date of final exams. |
| `total_assessment_attendees` | INTEGER | Number of students who took the exam. |
| `total_certified` | INTEGER | Number of students who passed and were certified. |
| `cert_vs_initial_enroll` | NUMERIC | % Ratio: Certified / Initial Enrollments. |
| `cert_vs_end_enroll` | NUMERIC | % Ratio: Certified / Final Enrollments. |
| `cert_vs_first_class_attend` | NUMERIC | % Ratio: Certified / First Class Attendees. |
| `cert_vs_avg_attend` | NUMERIC | % Ratio: Certified / Average Attendees. |
| `first_class_attend_vs_initial` | NUMERIC | % Ratio: First Class Attend / Initial Enroll. |
| `first_class_attend_vs_last` | NUMERIC | % Ratio: First Class Attend / Last Class Attend. |
| `pass_pct_cert_vs_attendees` | NUMERIC | % Ratio: Certified / Assessment Attendees. |
| `pass_pct_students_vs_cert` | NUMERIC | % Ratio: Final Count / Certified. |
| `overall_rating` | NUMERIC | Average student rating (1-5). |
| `avg_rating_ease` | NUMERIC | Rating for ease of attending webinars. |
| `avg_rating_quality` | NUMERIC | Rating for content quality. |
| `avg_teacher_rating` | NUMERIC | Rating for the teacher. |
| `avg_rating_access` | NUMERIC | Rating for website ease of use. |
| `avg_ela_rating` | NUMERIC | Teacher's rating for ELA. |
| `avg_content_support_rating` | NUMERIC | Teacher's rating for content support. |
| `is_latest_batch` | SMALLINT | 1 if this is the newest batch. |
| `include_in_course_count` | SMALLINT | 1 if this is a primary course. |
| `has_batch` | SMALLINT | 1 if a batch exists in the system. |
| `created_at` | TIMESTAMPTZ | Record creation timestamp in IST. |


---

## silver.batches_data

One row per batch. Populated daily by `fetch_course_batches.py`. Upsert key: `batch_id`.

| Column | Type | Description |
|---|---|---|
| `batch_id` | BIGINT | Edmingle batch identifier |
| `batch_name` | TEXT | Batch name |
| `bundle_id` | BIGINT | Parent bundle |
| `bundle_name` | TEXT | bundle name |
| `batch_status` | TEXT | active / archived / completed |
| `start_date_ist` | TIMESTAMPTZ | Batch start date in IST |
| `end_date_ist` | TIMESTAMPTZ | Batch end date in IST |
| `tutor_id` | BIGINT | Primary teacher ID |
| `tutor_name` | TEXT | Primary teacher name |
| `admitted_students` | INTEGER | Total students enrolled |

---

## silver.attendance_data

One row per batch per class date. Populated daily by `fetch_attendance.py`. Upsert key: `(batch_id, class_date)`.
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `bundle_id` | INTEGER | Edmingle course bundle ID. Foreign key to `course_batch_merge.bundle_id` and `course_catalogue_data.Bundle id` | `27250` |
| `bundle_name` | TEXT | Name of the parent course bundle | `test course` |
| `batch_id` | INTEGER | Edmingle batch ID. Foreign key to `course_batch_merge.batch_id` and `batches_data.batch_id` | `70610` |
| `batch_name` | TEXT | Name of the batch this class belongs to | `Test Course` |
| `class_id` | INTEGER | Edmingle class (session) ID. Unique identifier for a single scheduled session | `199329` |
| `attendance_id` | INTEGER | Internal Edmingle ID for this attendance record | `5783151` |
| `class_date` | DATE | Date the class was held, in `YYYY-MM-DD` format | `2026-05-04` |
| `class_duration` | TEXT | Duration of the class session | `1 hour` |
| `teacher_id` | INTEGER | Edmingle user ID of the teacher who conducted the session | `49815891` |
| `teacher_name` | TEXT | Name of the teacher who conducted the session | `Dr. Maheshwari H` |
| `present_count` | INTEGER | Number of students marked present for this session | `1` |
| `absent_count` | INTEGER | Number of students marked absent for this session | `3` |
| `total_records` | INTEGER | Total number of student records for this session (`present_count + absent_count`) | `4` |
| `attendance_pct` | FLOAT | Attendance percentage for this session (`present_count / total_records * 100`) | `25.0` |
| `pull_date` | DATE | Date this data was pulled from Edmingle, in `YYYY-MM-DD` format | `2026-05-04` |
| `loaded_at` | TIMESTAMP | Datetime when this row was inserted into the database | `2026-05-05 00:52:09` |