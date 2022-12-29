--check if datasets are well named and working

SELECT *
FROM PortfolioProject..covid_deaths
ORDER BY 3,4


SELECT *
FROM PortfolioProject..covid_vaccination
ORDER BY 3,4

-- select the colunms that we will need

SELECT location, date, population, total_cases, new_cases, total_deaths
FROM PortfolioProject..covid_deaths
ORDER BY 1,2

-- Analysing the total number of cases vs total number of deaths
-- See the scenario in my country

SELECT location, date, total_cases, total_deaths, (total_deaths/ total_cases)*100 AS death_percent
FROM PortfolioProject..covid_deaths
WHERE location like '%Portugal%'
ORDER BY 1,2

-- Analysing the total number of cases vs population

SELECT location, date, population, total_cases, total_deaths, (total_cases/ population)*100 AS infected_perct
FROM PortfolioProject..covid_deaths
WHERE population IS NOT NULL AND total_cases IS NOT NULL AND location like '%Portugal%'  -- removing null rows


-- Looking at world maximuns
-- Which country has the highest infection rate

SELECT location, MAX(total_cases/population *100) as highest_infection_rate
FROM PortfolioProject..covid_deaths
WHERE population IS NOT NULL AND total_cases IS NOT NULL
GROUP BY location
ORDER BY highest_infection_rate DESC
	
-- which countries have the highest death count
-- discovering some problems with the dataset on this query: 
--    > changing total_deaths to integer using CAST function
---   > There is location "World", "Europe", "High income", etc. Add the WHERE clause  "continent" to NOT NULL to fix this!

SELECT location, MAX(CAST(total_deaths as int)) as death_counts, MAX(total_deaths/population *100) as Total_deaths_perct
FROM PortfolioProject..covid_deaths
WHERE population IS NOT NULL AND total_cases IS NOT NULL AND continent IS NOT NULL
GROUP BY location
ORDER BY Total_deaths_perct DESC  


-- and what about per continent??

SELECT continent, MAX(CAST(total_deaths as int)) as death_counts, MAX(total_deaths/population *100) as Total_deaths_perct
FROM PortfolioProject..covid_deaths
WHERE population IS NOT NULL AND total_cases IS NOT NULL AND continent IS NOT NULL
GROUP BY continent
ORDER BY Total_deaths_perct DESC 


-- Retrieving GLOBAL COVID numbers over time

SELECT date, SUM(new_cases) as world_new_cases, SUM(CAST(new_deaths as int)) as world_Total_deaths, SUM(CAST(new_deaths as int)) /SUM(new_cases) *100 as world_death_perc
FROM PortfolioProject..covid_deaths
WHERE population IS NOT NULL AND total_cases IS NOT NULL AND continent IS NOT NULL
GROUP BY date
ORDER BY 1

-- the REAL GLOBAL NUMBER of infections, deaths and world global percentage:

	
SELECT SUM(new_cases) as world_new_cases, SUM(CAST(new_deaths as int)) as world_Total_deaths, SUM(CAST(new_deaths as int)) /SUM(new_cases) *100 as world_death_perc
FROM PortfolioProject..covid_deaths
WHERE population IS NOT NULL AND total_cases IS NOT NULL AND continent IS NOT NULL


-- NOW, let's join the table of vacination data to analyse both information together and look for some insights on the vacination effect

-- joining the two tables:

-- > fixed problems (convert new_vaccinations to big integer because it gets very big)

SELECT d.continent, d.location, d.date, d.population, v.new_vaccinations, 
	SUM(Cast(v.new_vaccinations as bigint)) OVER (PARTITION BY d.location ORDER BY d.location, d.date) as accumulated_vacination --- partition gives a rolling counting, in this case, for each locatino and ordered by location and date
FROM PortfolioProject..covid_deaths d
JOIN PortfolioProject..covid_vaccination v
	ON d.location = v.location
	AND d.date = v.date
WHERE d.continent IS NOT NULL AND v.new_vaccinations IS NOT NULL
ORDER BY 1,2,3;


-- Using CTE to reuse new colunms in our analysis (creating a temporary table with new calculated colunmns)
-- (note: the number of columns in the CTE must be the same that of the Select statement)

WITH PopvsVac(continent, location, population, new_vaccinations, people_vaccinated, accumulated_vacination)
as
	(
	SELECT d.continent, d.location, d.population, v.new_vaccinations, v.people_vaccinated,
	SUM(CAST(v.people_vaccinated as bigint)) OVER (PARTITION BY d.location ORDER BY d.location) as accumulated_vacination--- partition gives a rolling counting, in this case, for each locatino and ordered by location and date
	FROM PortfolioProject..covid_deaths d
	JOIN PortfolioProject..covid_vaccination v
		ON d.location = v.location
		AND d.date = v.date
	WHERE d.continent IS NOT NULL AND v.new_vaccinations IS NOT NULL
	)
SELECT location, MAX(people_vaccinated/population*100) as PercPopVac
FROM PopvsVac
WHERE population IS NOT NULL AND accumulated_vacination IS NOT NULL
GROUP BY location
ORDER BY PercPopVac



-- USING A TEMPORARY TABLE TO EXPLORE NEW COLUMN CALCULATIONS

DROP TABLE if exists #PopulationTotalVaccination
Create table #PopulationTotalVaccination
(continent nvarchar(255),
location nvarchar(255),
date datetime,
population numeric,
people_vaccinated numeric,
accumulated_vacination numeric
)
INSERT INTO #PopulationTotalVaccination
	SELECT d.continent, d.location, d.date, d.population, v.people_vaccinated,
	SUM(Cast(v.people_vaccinated as bigint)) OVER (PARTITION BY d.location ORDER BY d.location, d.date) as accumulated_vacination--- partition gives a rolling counting, in this case, for each locatino and ordered by location and date
	FROM PortfolioProject..covid_deaths d
	JOIN PortfolioProject..covid_vaccination v
		ON d.location = v.location
		AND d.date = v.date
	WHERE d.continent IS NOT NULL AND v.people_vaccinated IS NOT NULL

SELECT *, (people_vaccinated/population)*100 as PercPopVac
FROM #PopulationTotalVaccination



--CREATING VIEWS FOR VISUALIZATION ON TABLEAU



CREATE VIEW COVID_Death_perc as
SELECT location, date, total_cases, total_deaths, (total_deaths/ total_cases)*100 AS death_percent
FROM PortfolioProject..covid_deaths
--WHERE location like '%Portugal%'
--ORDER BY 1,2
--GROUP BY location

CREATE VIEW InfectionRates as
SELECT location, MAX(total_cases/population *100) as highest_infection_rate
FROM PortfolioProject..covid_deaths
WHERE population IS NOT NULL AND total_cases IS NOT NULL
GROUP BY location

CREATE VIEW DeathRates as
SELECT location, MAX(CAST(total_deaths as int)) as death_counts, MAX(total_deaths/population *100) as Total_deaths_perct
FROM PortfolioProject..covid_deaths
WHERE population IS NOT NULL AND total_cases IS NOT NULL AND continent IS NOT NULL
GROUP BY location 

CREATE VIEW CovidDeathperContinent as
SELECT continent, MAX(CAST(total_deaths as int)) as death_counts, MAX(total_deaths/population *100) as Total_deaths_perct
FROM PortfolioProject..covid_deaths
WHERE population IS NOT NULL AND total_cases IS NOT NULL AND continent IS NOT NULL
GROUP BY continent

CREATE VIEW WorldDeathsOverTime as
SELECT date, SUM(new_cases) as world_new_cases, SUM(CAST(new_deaths as int)) as world_Total_deaths, SUM(CAST(new_deaths as int)) /SUM(new_cases) *100 as world_death_perc
FROM PortfolioProject..covid_deaths
WHERE population IS NOT NULL AND total_cases IS NOT NULL AND continent IS NOT NULL
GROUP BY date

CREATE VIEW WorldlCovid as
SELECT SUM(new_cases) as world_new_cases, SUM(CAST(new_deaths as int)) as world_Total_deaths, SUM(CAST(new_deaths as int)) /SUM(new_cases) *100 as world_death_perc
FROM PortfolioProject..covid_deaths
WHERE population IS NOT NULL AND total_cases IS NOT NULL AND continent IS NOT NULL


CREATE VIEW WorldPopVaccination as
WITH PopvsVac(continent, location, population, new_vaccinations, people_vaccinated, accumulated_vacination)
as
	(
	SELECT d.continent, d.location, d.population, v.new_vaccinations, v.people_vaccinated,
	SUM(CAST(v.people_vaccinated as bigint)) OVER (PARTITION BY d.location ORDER BY d.location) as accumulated_vacination--- partition gives a rolling counting, in this case, for each locatino and ordered by location and date
	FROM PortfolioProject..covid_deaths d
	JOIN PortfolioProject..covid_vaccination v
		ON d.location = v.location
		AND d.date = v.date
	WHERE d.continent IS NOT NULL AND v.new_vaccinations IS NOT NULL
	)
SELECT location, MAX(people_vaccinated/population*100) as PercPopVac
FROM PopvsVac
WHERE population IS NOT NULL AND accumulated_vacination IS NOT NULL
GROUP BY location