# def generate_ride_sharing_erd():
#     erd = """erDiagram
#     DRIVERS {
#         string driver_id PK
#         string first_name
#         string last_name
#         string email
#         string phone_number
#         string license_number
#         date license_expiry_date
#         string vehicle_make
#         string vehicle_model
#         string vehicle_year
#         string registration_number
#         string current_status
#         date registration_date
#         float rating
#     }
#     RIDERS {
#         string rider_id PK
#         string first_name
#         string last_name
#         string email
#         string phone_number
#         date registration_date
#         string home_address
#         float rating
#     }
#     ORDERS {
#         string order_id PK
#         string rider_id FK
#         string pickup_location
#         string dropoff_location
#         string order_status
#         decimal estimated_fare
#         timestamp created_at
#         string payment_method_id FK
#         string cancellation_reason
#     }
#     RIDES {
#         string ride_id PK
#         string order_id FK
#         string driver_id FK
#         timestamp start_time
#         timestamp end_time
#         string actual_pickup_location
#         string actual_dropoff_location
#         decimal distance_traveled
#         decimal actual_fare
#         string ride_status
#         int rider_rating
#         int driver_rating
#         string comments
#     }
#     PAYMENT_METHODS {
#         string payment_method_id PK
#         string rider_id FK
#         string payment_type
#         string card_last4
#         date expiry_date
#         boolean is_default
#     }
#     DRIVERS ||--o{ RIDES : provides
#     RIDERS ||--o{ ORDERS : places
#     RIDERS ||--o{ PAYMENT_METHODS : has
#     ORDERS ||--|| RIDES : fulfills
#     PAYMENT_METHODS ||--o{ ORDERS : used_for"""
#
#     with open('ride_sharing_erd.md', 'w') as f:
#         f.write(erd)
#
#
# generate_ride_sharing_erd()
# First, create or modify generate_erd.py
def generate_ride_sharing_erd():
    erd = """```mermaid
erDiagram
    DRIVERS {
        string driver_id PK
        string first_name
        string last_name
        string email
        string phone_number
        string license_number
        date license_expiry_date
        string vehicle_make
        string vehicle_model
        string vehicle_year
        string registration_number
        string current_status
        date registration_date
        float rating
    }
    RIDERS {
        string rider_id PK
        string first_name
        string last_name
        string email
        string phone_number
        date registration_date
        string home_address
        float rating
    }
    ORDERS {
        string order_id PK
        string rider_id FK
        string pickup_location
        string dropoff_location
        string order_status
        decimal estimated_fare
        timestamp created_at
        string payment_method_id FK
        string cancellation_reason
    }
    RIDES {
        string ride_id PK
        string order_id FK
        string driver_id FK
        timestamp start_time
        timestamp end_time
        string actual_pickup_location
        string actual_dropoff_location
        decimal distance_traveled
        decimal actual_fare
        string ride_status
        int rider_rating
        int driver_rating
        string comments
    }
    PAYMENT_METHODS {
        string payment_method_id PK
        string rider_id FK
        string payment_type
        string card_last4
        date expiry_date
        boolean is_default
    }
    DRIVERS ||--o{ RIDES : provides
    RIDERS ||--o{ ORDERS : places
    RIDERS ||--o{ PAYMENT_METHODS : has
    ORDERS ||--|| RIDES : fulfills
    PAYMENT_METHODS ||--o{ ORDERS : used_for
```"""

    with open('ride_sharing_erd.md', 'w') as f:
        f.write(erd)


# Run the function
generate_ride_sharing_erd()