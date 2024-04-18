SELECT
    ss.CLIENT_ID,
    ROW_NUMBER() OVER (PARTITION BY ss.CLIENT_ID ORDER BY oo.CLIENT_NAME asc) as SERIAL_NUMBER,
    oo.CLIENT_NAME,
    oo.PHONE,
    oo.STATE,
    CASE
           WHEN state = 'Alabama' THEN 'US-AL'
           WHEN state = 'Alaska' THEN 'US-AK'
           WHEN state = 'Arizona' THEN 'US-AZ'
           WHEN state = 'Arkansas' THEN 'US-AR'
           WHEN state = 'California' THEN 'US-CA'
           WHEN state = 'Colorado' THEN 'US-CO'
           WHEN state = 'Connecticut' THEN 'US-CT'
           WHEN state = 'Delaware' THEN 'US-DE'
           WHEN state = 'Florida' THEN 'US-FL'
           WHEN state = 'Georgia' THEN 'US-GA'
           WHEN state = 'Hawaii' THEN 'US-HI'
           WHEN state = 'Idaho' THEN 'US-ID'
           WHEN state = 'Illinois' THEN 'US-IL'
           WHEN state = 'Indiana' THEN 'US-IN'
           WHEN state = 'Iowa' THEN 'US-IA'
           WHEN state = 'Kansas' THEN 'US-KS'
           WHEN state = 'Kentucky' THEN 'US-KY'
           WHEN state = 'Louisiana' THEN 'US-LA'
           WHEN state = 'Maine' THEN 'US-ME'
           WHEN state = 'Maryland' THEN 'US-MD'
           WHEN state = 'Massachusetts' THEN 'US-MA'
           WHEN state = 'Michigan' THEN 'US-MI'
           WHEN state = 'Minnesota' THEN 'US-MN'
           WHEN state = 'Mississippi' THEN 'US-MS'
           WHEN state = 'Missouri' THEN 'US-MO'
           WHEN state = 'Montana' THEN 'US-MT'
           WHEN state = 'Nebraska' THEN 'US-NE'
           WHEN state = 'Nevada' THEN 'US-NV'
           WHEN state = 'New Hampshire' THEN 'US-NH'
           WHEN state = 'New Jersey' THEN 'US-NJ'
           WHEN state = 'New Mexico' THEN 'US-NM'
           WHEN state = 'New York' THEN 'US-NY'
           WHEN state = 'North Carolina' THEN 'US-NC'
           WHEN state = 'North Dakota' THEN 'US-ND'
           WHEN state = 'Ohio' THEN 'US-OH'
           WHEN state = 'Oklahoma' THEN 'US-OK'
           WHEN state = 'Oregon' THEN 'US-OR'
           WHEN state = 'Pennsylvania' THEN 'US-PA'
           WHEN state = 'Rhode Island' THEN 'US-RI'
           WHEN state = 'South Carolina' THEN 'US-SC'
           WHEN state = 'South Dakota' THEN 'US-SD'
           WHEN state = 'Tennessee' THEN 'US-TN'
           WHEN state = 'Texas' THEN 'US-TX'
           WHEN state = 'Utah' THEN 'US-UT'
           WHEN state = 'Vermont' THEN 'US-VT'
           WHEN state = 'Virginia' THEN 'US-VA'
           WHEN state = 'Washington' THEN 'US-WA'
           WHEN state = 'West Virginia' THEN 'US-WV'
           WHEN state = 'Wisconsin' THEN 'US-WI'
           WHEN state = 'Wyoming' THEN 'US-WY'
           ELSE NULL
       end AS STATE_CODE,
    oo.SHIPPING_ADDRESS,
    ss.OS,
    ss.IP
    
FROM {{ ref('base_web_sessions') }} AS ss
LEFT JOIN {{ ref('base_web_orders') }} AS oo
ON ss.SESSION_ID = oo.SESSION_ID
where CLIENT_NAME IS NOT NULL
order by 1, 3
