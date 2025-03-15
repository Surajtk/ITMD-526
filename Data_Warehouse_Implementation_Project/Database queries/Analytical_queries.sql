--- Top 5 Car Models by Sales
SELECT
    cmd.MODEL,
    SUM(csf.SALES_IN_THOUSANDS) AS Total_Sales
FROM
    CAR_SALES_FACT csf
JOIN
    CAR_MODEL_DIMENSION cmd ON csf.MODEL_ID = cmd.MODEL_ID
GROUP BY
    cmd.MODEL
ORDER BY
    Total_Sales DESC
LIMIT 5;

--- Average Safety Rating by Car Make
SELECT
    cmd.MAKE,
    AVG(srd.OVERALL_STARS) AS Average_Safety_Rating
FROM
    SAFER_CARS_FACT scf
JOIN
    CAR_MAKE_DIMENSION cmd ON scf.MAKE_ID = cmd.MAKE_ID
JOIN
    SAFETY_RATING_DIMENSION srd ON scf.SAFETY_ID = srd.SAFETY_ID
GROUP BY
    cmd.MAKE
ORDER BY
    Average_Safety_Rating DESC
LIMIT 20;

--- Average Highway MPG by Engine Fuel Type
SELECT
    ed.ENGINE_FUEL_TYPE,
    AVG(cff.HIGHWAY_MPG) AS Average_Highway_MPG
FROM
    CAR_FEATURES_FACT cff
JOIN
    ENGINE_DIMENSION ed ON cff.ENGINE_ID = ed.ENGINE_ID
GROUP BY
    ed.ENGINE_FUEL_TYPE
ORDER BY 
    Average_Highway_MPG DESC;
    
---Total Sales by Car Make
SELECT
    cmd.MAKE,
    SUM(csf.SALES_IN_THOUSANDS) AS Total_Sales
FROM
    CAR_SALES_FACT csf
JOIN
    CAR_MAKE_DIMENSION cmd ON csf.MAKE_ID = cmd.MAKE_ID
GROUP BY
    cmd.MAKE
ORDER BY
    Total_Sales DESC;

--- Models with High Sales and High Safety Ratings
SELECT
    cmd.MODEL,
    SUM(csf.SALES_IN_THOUSANDS) AS Total_Sales,
    AVG(srd.OVERALL_STARS) AS Average_Safety_Rating
FROM
    CAR_SALES_FACT csf
JOIN
    CAR_MODEL_DIMENSION cmd ON csf.MODEL_ID = cmd.MODEL_ID
JOIN
    SAFER_CARS_FACT scf ON cmd.MODEL_ID = scf.MODEL_ID
JOIN
    SAFETY_RATING_DIMENSION srd ON scf.SAFETY_ID = srd.SAFETY_ID
GROUP BY
    cmd.MODEL
HAVING
    Total_Sales > (SELECT AVG(SALES_IN_THOUSANDS) FROM CAR_SALES_FACT)
    AND Average_Safety_Rating > 4
ORDER BY
    Total_Sales DESC;

