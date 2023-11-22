SELECT identity_verification_id,
       active,
       DATE_ADD('1970-01-01', INTERVAL FLOOR(RAND() * (DATEDIFF('2000-12-31', '1970-01-01')+1)) DAY) AS birthday, -- Eg: 1987-05-21
       LPAD(FLOOR(RAND() * POW(10, 88)), 88, '0')                                                    AS ci,       -- Random 88-digit number
       LPAD(FLOOR(RAND() * POW(10, 64)), 64, '0')                                                    AS di,       -- Random 64-digit number
       name,
       gender,
       CONCAT('010', LPAD(FLOOR(RAND() * 90000000) + 10000000, 8, '0'))                              AS phone,    -- Eg: 01012345678
       customer_id,
       created_at,
       updated_at
FROM identity_verification;
