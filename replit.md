# GTN Engineering Contract Review Dashboard

## Overview
Flask + SQLite application for managing multi-PO (Purchase Order) contract review with role-based access control for GTN Engineering (India) Limited.

**Current State**: Fully functional Flask backend with SQLite database, user authentication, PO management, and three form types (CR, PED, LEAD).

**Last Updated**: November 5, 2025

## Recent Changes
- **November 5, 2025: CR Excel Export Feature**
  - Implemented Excel export functionality for Contract Review (CR) forms
  - Exports all CR form data (header info + row details) from database
  - Automatically splits data into 3 separate Excel files by data range for easy printing
  - All 3 files are packaged into a ZIP file (CR_Export.zip) for download
  - Professional formatting with headers, borders, cell styling, and auto-adjusted column widths
  - Empty files display "No CR forms in this range" message
  - Export button available on Master Dashboard (Admin users can access)
  - New API endpoint: GET /api/cr-export-excel
  - Uses openpyxl library for Excel file generation

- **November 5, 2025: LEAD Form Auto-Save Functionality**
  - Implemented complete auto-save functionality for LEAD Time Calculation Sheet
  - Added database tables (lead_forms and lead_form_rows) to persist LEAD form data
  - Created backend API endpoints (/api/lead-form/save and /api/lead-form/load)
  - Auto-save triggers 2 seconds after user stops typing (consistent with CR and PED forms)
  - Auto-refresh every 5 seconds to show changes from other admin users
  - Auto-refresh reconciles row additions/deletions made by other admins
  - LEAD form data now persists in database (previously only export was available)
  - Admin-only auto-save functionality - only admins can save/edit forms
  - Data includes: customer, bid, po, cr, record info, part details, dates, lead times, and remarks
  - Visual save status indicator shows: "Saving...", "✓ Auto-saved by username", or "Error saving"
  - All three forms (CR, PED, LEAD) now have identical auto-save behavior

- **November 5, 2025: Amendment Details Auto-Save with Admin-Only Edit**
  - Implemented auto-save functionality for Amendment Details field in both CR and PED forms
  - Added amendment_details column to cr_forms table
  - Created ped_forms and ped_form_rows tables with full auto-save support
  - Amendment Details field is now:
    - Editable only by Admin users (read-only for non-admins)
    - Server-side enforcement prevents API bypass
    - Auto-saves 2 seconds after typing stops
    - Loads automatically when form opens
  - Both CR and PED forms now have complete auto-save including amendment details

- **November 4, 2025: Amendment Details Editable Field**
  - Added dedicated textarea field for Admin to enter Amendment Details in CR and PED forms
  - Field appears below the instructional "Note:" section
  - Styled with proper focus effects and responsive sizing
  - Admin can enter custom amendment details that will be visible to all users

- **November 4, 2025: Auto-Save Feature for Contract Review Forms**
  - Implemented real-time auto-save functionality for CR forms
  - Added database tables (cr_forms, cr_form_rows) to persist form data
  - Auto-save triggers 2 seconds after user stops typing
  - Auto-refresh every 5 seconds to show changes from other users
  - Concurrent edit protection: prevents data loss when multiple users edit simultaneously
  - Visual indicators show save/load status in real-time
  - Failed saves automatically retry
  - All form data is now stored in database and visible to all users

- November 3, 2025: Initial Flask + SQLite implementation
  - Created Flask backend with session-based authentication
  - Implemented SQLite database schema for users and POs
  - Migrated all frontend files to static folder
  - Updated JavaScript files to use Flask API endpoints instead of localStorage
  - Preserved all original HTML/CSS/JS files exactly as provided
  - Configured Flask workflow to run on port 5000
  - **Security Fix**: Fixed critical backup/restore vulnerability
    - Backup now includes password_hash field to preserve original credentials
    - Restore validates password_hash format (pbkdf2/scrypt/bcrypt prefixes)
    - Added transaction safety with rollback on error
    - Prevents credential injection attacks

## Project Architecture

### Backend (Flask + SQLite)
- **app.py**: Main Flask application
  - Session-based authentication with werkzeug password hashing
  - Role-based access control (Admin vs non-Admin users)
  - RESTful API endpoints for users, POs, backup/restore
  - SQLite database with users and pos tables
  - Default admin user: username=`admin`, password=`admin`

### Database Schema
1. **users table**:
   - id (PRIMARY KEY)
   - username (UNIQUE, NOT NULL)
   - password_hash (NOT NULL)
   - name (NOT NULL)
   - department (NOT NULL)
   - is_admin (BOOLEAN, DEFAULT 0)
   - created_at (TIMESTAMP)

2. **pos table**:
   - id (PRIMARY KEY)
   - customer (NOT NULL)
   - bid (NOT NULL)
   - po (NOT NULL)
   - cr (NOT NULL)
   - created_at, updated_at (TIMESTAMP)

3. **cr_forms table** (Auto-save feature):
   - id (PRIMARY KEY)
   - po_key (UNIQUE, NOT NULL) - Composite key: customer|bid|po|cr
   - customer, bid, po, cr (form header fields)
   - record_no, record_date (TEXT)
   - amendment_details (TEXT, admin-only editable)
   - last_modified_by (username)
   - last_modified_at (TIMESTAMP)

4. **cr_form_rows table** (Auto-save feature):
   - id (PRIMARY KEY)
   - cr_form_id (FOREIGN KEY to cr_forms)
   - item_no (NOT NULL)
   - part_number, part_description, rev, qty (TEXT)
   - cycles (JSON array of 62 cycle values)
   - remarks (TEXT)

5. **ped_forms table** (Auto-save feature):
   - id (PRIMARY KEY)
   - po_key (UNIQUE, NOT NULL) - Composite key: customer|bid|po|cr
   - customer, bid, po, cr (form header fields)
   - record_no, record_date (TEXT)
   - amendment_details (TEXT, admin-only editable)
   - last_modified_by (username)
   - last_modified_at (TIMESTAMP)

6. **ped_form_rows table** (Auto-save feature):
   - id (PRIMARY KEY)
   - ped_form_id (FOREIGN KEY to ped_forms)
   - item_no (NOT NULL)
   - part_number, part_description, rev, qty (TEXT)
   - ped_cycles (JSON array of 11 PED cycle values)
   - notes (JSON array of 7 department note values)
   - remarks (TEXT)

7. **lead_forms table** (Auto-save feature):
   - id (PRIMARY KEY)
   - po_key (UNIQUE, NOT NULL) - Composite key: customer|bid|po|cr
   - customer, bid, po, cr (form header fields)
   - record_no, record_date (TEXT)
   - last_modified_by (username)
   - last_modified_at (TIMESTAMP)

8. **lead_form_rows table** (Auto-save feature):
   - id (PRIMARY KEY)
   - lead_form_id (FOREIGN KEY to lead_forms)
   - item_no (NOT NULL)
   - part_number, part_description, rev, qty (TEXT)
   - customer_required_date (TEXT)
   - standard_lead_time (TEXT)
   - gtn_agreed_date (TEXT)
   - remarks (TEXT)

### Frontend (Static Files)
All original HTML/CSS/JS files preserved in `static/` folder:
- **Login**: login.html, login_styles.css, login_script.js
- **Master Dashboard**: master.html, master_styles.css, master_script.js
- **User Management**: manage_users.html, manage_users_styles.css, manage_users_script.js
- **CR Form**: CR_index_all.html, CR_styles_all.css, CR_script_all.js
- **PED Form**: PED_index.html, PED_styles.css, PED_script.js
- **LEAD Form**: LEAD_index.html, LEAD_styles.css, LEAD_script.js

### API Endpoints
- `POST /api/login` - User login
- `POST /api/logout` - User logout
- `GET /api/session` - Get current session
- `GET /api/users` - List all users (Admin only)
- `POST /api/users` - Create new user (Admin only)
- `DELETE /api/users/<id>` - Delete user (Admin only)
- `GET /api/pos` - List all POs
- `POST /api/pos` - Create new PO (Admin only)
- `PUT /api/pos/<id>` - Update PO (Admin only)
- `DELETE /api/pos/<id>` - Delete PO (Admin only)
- `GET /api/backup` - Export users and POs as JSON (Admin only)
- `POST /api/restore` - Restore users and POs from backup (Admin only)
- `POST /api/cr-form/save` - Auto-save CR form data (Authenticated users)
- `GET /api/cr-form/load` - Load saved CR form data (Authenticated users)
- `GET /api/cr-export-excel` - Export all CR forms to 3 Excel files in ZIP (Authenticated users)
- `POST /api/ped-form/save` - Auto-save PED form data (Authenticated users)
- `GET /api/ped-form/load` - Load saved PED form data (Authenticated users)
- `POST /api/lead-form/save` - Save LEAD form data (Admin only)
- `GET /api/lead-form/load` - Load saved LEAD form data (Authenticated users)

## User Roles and Permissions

### Admin Users
- Full access to all features
- Can add, edit, delete POs
- Can manage users (create, delete)
- Can backup and restore data
- Can open and edit all forms (CR, PED, LEAD)

### Non-Admin Users
- Read-only access to PO dashboard
- Can view POs and open forms
- Cannot add, edit, or delete POs
- Cannot manage users
- Cannot backup or restore data
- Form editing permissions:
  - **CR Form**: Can edit only their department's cycle columns
  - **PED Form**: Can edit only their department's PED cycles and note cells
  - **LEAD Form**: Read-only (Admin-editable only)

## Department List
1. IT
2. CSS
3. Engineering (9 CR cycles, 8 PED cycles)
4. Manufacturing (8 CR cycles, 1 PED cycle)
5. Materials/Planning (8 CR cycles, 1 PED cycle)
6. Purchase (6 CR cycles, 1 PED cycle)
7. Special Process (4 CR cycles)
8. Welding (3 CR cycles)
9. Assembly & Testing (4 CR cycles)
10. Quality (10 CR cycles)
11. Painting/Despatch (5 CR cycles)
12. Customer Service & Sales (4 CR cycles)
13. Commercial (1 CR cycle)

## Form Types
1. **Contract Review (CR)**: 62 cycle columns mapped to 11 departments
2. **PED Review**: 11 PED cycles + 7 department note cells
3. **Lead Time**: Admin-editable, read-only for others

## Security Features
- Password hashing with werkzeug.security
- Session-based authentication using Flask sessions
- CSRF protection via session secrets
- Role-based access control enforced on both frontend and backend
- Safety checks: Cannot delete last admin user or default admin
- Backup/restore validates admin user presence

## Running the Application
1. Flask server runs on port 5000 (configured in workflow)
2. Default credentials: `admin` / `admin`
3. Database automatically initialized on first run
4. Access via web browser at the Replit URL

## User Preferences
- All original HTML/CSS/JS files must be preserved exactly as provided
- No modifications to the frontend structure allowed
- Backend handles authentication and data storage
- Frontend uses localStorage only for session info, all data from API
