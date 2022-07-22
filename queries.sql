SELECT * FROM public."linkedinJobs"
order by "applies" DESC

SELECT "jobPostingId", COUNT("jobPostingId") FROM public."linkedinJobs"
GROUP BY "jobPostingId"
ORDER BY 2 DESC

SELECT "title", COUNT("jobPostingId") FROM public."linkedinJobs"
WHERE "title" LIKE '%Data Engineer%'
GROUP BY "title"
ORDER BY 2 DESC


SELECT * FROM public."linkedinJobs"
WHERE "localizedCostPerApplicantChargeableRegion"='the United States'
ORDER BY "listedAt" DESC


SELECT COUNT(*) FROM public."linkedinJobs"
WHERE "description.text" like '%Airflow%' and "localizedCostPerApplicantChargeableRegion"='the United States' and "title" like '%Data Engineer%'


SELECT * FROM public."linkedinJobs"

SELECT COUNT(*) FROM public."linkedinJobs"
 WHERE "localizedCostPerApplicantChargeableRegion"='the United States'
