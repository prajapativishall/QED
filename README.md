# QED Utility Portal

A Django-based utility portal designed for QED project management, providing data visualization, bulk operations, and role-based access control.

## Features

- **Dashboard**: Real-time insights into process instances (connecting to external workflow databases), summarized by Circle and Activity.
- **Bulk Operations**:
  - **Bulk Upload**: Upload data via Excel templates.
  - **Bulk Delete**: Efficiently remove records using Excel-based input.
- **Data Export**: Export filtered data for reporting.
- **Role-Based Access Control (RBAC)**: Secure access management using Django's group permissions.

## Prerequisites

- Python 3.8+
- MySQL Database (for the external process engine data)

## Installation

1.  **Clone the repository**

    ```bash
    git clone https://github.com/prajapativishall/QED.git
    cd QED/qed_utility
    ```

2.  **Create and activate a virtual environment**

    ```bash
    # Windows
    python -m venv .venv
    .venv\Scripts\activate

    # Linux/Mac
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3.  **Install dependencies**

    ```bash
    pip install -r requirements.txt
    ```

4.  **Environment Configuration**

    Create a `.env` file in the project root (`d:\QACA\QED Portal\qed_utility\.env`) and add the following configurations:

    ```ini
    # Database Configuration (for Dashboard)
    DB_HOST=your_db_host
    DB_USER=your_db_user
    DB_PASSWORD=your_db_password
    DB_NAME=your_db_name

    # Django Settings (Optional overrides)
    # DEBUG=True
    # SECRET_KEY=your_secret_key
    ```

5.  **Initialize the Database**

    ```bash
    python manage.py migrate
    ```

6.  **Setup Roles**

    Initialize the default roles/groups:

    ```bash
    python manage.py setup_roles
    ```

7.  **Create a Superuser** (for Admin access)

    ```bash
    python manage.py createsuperuser
    ```

## Usage

Start the development server:

```bash
python manage.py runserver
```

Visit `http://127.0.0.1:8000/` in your browser.

## Project Structure

- `qed_utility/`: Main application logic.
  - `views/`: Contains logic for Dashboard, Auth, Bulk Upload/Delete.
  - `management/commands/`: Custom management commands (e.g., `setup_roles`).
  - `static/`: CSS, images, and Excel templates.
  - `templates/`: HTML templates.
- `qed_site/`: Project-level settings and configuration.
