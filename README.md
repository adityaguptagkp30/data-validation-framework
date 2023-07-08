# data-validation-framework
This is framework which can validate the data checking the quality of data received from upstream and can flag the further orchestration of data in the silver and gold layers.
User can configure the required checks on the required location which could be silver container, bronze container, gold container or synapse.
It uses great expectation library for the different kinds of data quality checks.
Description:
• Framework which checks on different data quality checks within dataframes
like unexpected data, nulls, Fact Dim key checks, duplicates, percentage data
increase check, etc depending on the check inputs needed by userfor any
specific schema.
• Fail the further data orchestration in any of silver, gold or synapse layers and fail
other dependent jobs by sending a notification mail with reason of fault in data.
• Addons like storing logs of everyday runs and auto deleting 35+ days old data.
