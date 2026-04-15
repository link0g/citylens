CREATE OR REPLACE TABLE CITYLENS_MERGED_DB.CRIME_PUBLIC.DISTRICT_NEIGHBORHOOD_MAP AS
SELECT * FROM VALUES
    ('A1',  'Downtown'),
    ('A1',  'Beacon Hill'),
    ('A1',  'West End'),
    ('A1',  'North End'),
    ('A7',  'East Boston'),
    ('B2',  'Roxbury'),
    ('B2',  'South End'),
    ('B3',  'Mattapan'),
    ('B3',  'Hyde Park'),
    ('C6',  'South Boston'),
    ('C11', 'Dorchester'),
    ('D4',  'Fenway'),
    ('D4',  'Back Bay'),
    ('D4',  'South Boston Waterfront'),
    ('D14', 'Allston'),
    ('D14', 'Brighton'),
    ('E5',  'West Roxbury'),
    ('E13', 'Jamaica Plain'),
    ('E13', 'Roslindale'),
    ('E18', 'Hyde Park')
AS t(DISTRICT, NEIGHBORHOOD_NAME);