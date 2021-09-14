# ComponentsGDPbyIndustry.jl
#= This script pulls the data from:
- Industry Economic Account Data
	- GDP by Industry Components of Value Added
		- Value Added (A)
		- Compensation of employees (A)
		- Wages and Salaries (A)
		- Taxes on production and imports, less subsidies (A)
		- Gross operating surplus (A)
For all industries for years 2008 - 2019
Computes the NAICS2 sector code blow-up factors to be applied to salary and wages

## Source: https://apps.bea.gov/iTable/iTable.cfm?reqid=150&step=2&isuri=1&categories=compbyind
## Note: Endpoint is not available for consuming it from the API. We use the spreadsheet file.
=#
using HTTP: HTTP, request, URI
using JSON3: JSON3
using XLSX: XLSX, readxlsx, readtable, readdata, sheetnames
using CSV: CSV
using DataFrames: DataFrames, DataFrame, combine, groupby, ByRow, dropmissing!,
				  transform!, select!, select, leftjoin
const BEA_API_KEY = get(ENV, "api_bea_token", "")

#=
Download the information and format it.
=#
# Download the spreadsheet file for the components of value added
if !isfile(joinpath("data", "ComponentsOfVa.xlsx"))
	download(string(URI(scheme = "https",
						host = "apps.bea.gov",
						path = "/industry/Release/XLS/CompByInd/ComponentsOfVa.xlsx")),
		 	 joinpath("data", "ComponentsOfVa.xlsx"))
end
# Read the spreadsheet file
components_of_va = readxlsx(joinpath("data", "ComponentsOfVa.xlsx"))
# The first sheet has the name of the series to sheets mapping
tblnames = Dict(components_of_va[1]["C5:C17"] .=> sheetnames(components_of_va)[2:end])
# These are the cell range for all industries for years 2008 - 2019
range_for_industries_2008_2019 = "N11:Y96"
# Get the industry names
all_industries = convert(Vector{String}, vec(components_of_va[2]["B11:B96"]))
# Read the data for each variable and format it together with the industry and year
components_of_va_data =
	DataFrame(reduce(hcat,
					 vec([isa(x, Number) ? x : missing
					 	  for x in components_of_va[tblnames[k]][range_for_industries_2008_2019]])
						  for k in sort(collect(keys(tblnames)))),
			  sort!(collect(keys(tblnames))))
components_of_va_data[!,:industry] .= repeat(all_industries, outer = length(2008:2019))
components_of_va_data[!,:year] .= repeat(2008:2019, inner = length(all_industries))
components_of_va_data =
	components_of_va_data[!,union([:industry, :year], propertynames(components_of_va_data))]
transform!(components_of_va_data, :industry => ByRow(strip) => :industry)

#=
Find the mapping to of industry to NAICS2 (sector) through the APIs
=#
# url = URI(scheme = "https", host = "apps.bea.gov", path = "/api/data",
#           query = ["UserID" => BEA_API_KEY,
#                    "method" => "GetParameterList",
#                    "DatasetName" => "UnderlyingGDPbyIndustry",
#                    "ResultFormat" => "JSON"
#                   ])
# response = request("GET", url)
# @assert response.status == 200
# json = JSON3.read(response.body)
# json.BEAAPI.Results.Parameter
#=
Using the code above I found the ParameterName

Using a similar table, I can get the `Industry` list with the NAICS
TableID: 6 -- Components of Value Added by Industry (A)
Frequency: "A" -- Annual
=#
# url = URI(scheme = "https", host = "apps.bea.gov", path = "/api/data",
#           query = ["UserID" => BEA_API_KEY,
#                    "method" => "GetParameterValues",
#                    "DatasetName" => "UnderlyingGDPbyIndustry",
#                 #    "TableID" => 210,
#                    "ParameterName" => "Industry",
#                    "ResultFormat" => "JSON"
#         ])
# response = request("GET", url)
# @assert response.status == 200
# json = JSON3.read(response.body)
# industries = DataFrame(json.BEAAPI.Results.ParamValue)
# CSV.write(joinpath("data", "BEA_Industry_Naics.csv"), industries)
#=
We obtain a mapping from BEA industry to NAICS2.
=#
industries = CSV.read(joinpath("data", "BEA_Industry_Naics.csv"), DataFrame)
transform!(industries, :Key => ByRow(x -> match(r"\d+\-?\d+", x)) => :naics)
transform!(industries, :naics => ByRow(x -> isnothing(x) ? missing : x.match) => :naics)
dropmissing!(industries)
transform!(industries, :Desc => ByRow(x -> replace(x, " (A,Q)" => "")) => :industry)
select!(industries, [:naics, :industry])
industries = combine(groupby(industries, :naics), first)
#=
Join the data so we can obtain the relevant information at the NAICS2 level.
=#
naics = leftjoin(components_of_va_data, industries, on = :industry)
select!(naics,
		["year", "naics", "industry",
		 "Wages and salaries",
		 "Compensation of employees",
		 "Value Added",
		 "Gross operating surplus",
		 "Taxes on production and imports, less subsidies"])
dropmissing!(naics)
#=
We find that we need to handle these four cases:
- "31-33" use 31
- "44-45" use 44
- "48-49" use 48
- "99" do not include
=#
bea_oews = Dict("31" => "31-33", "44" => "44-45", "48" => "48-49")
#=
Compute simple salary and wages to resource cost factors per industry (NAICS2).
=#
comp_of_va = select(naics, ["year", "naics", "Wages and salaries", "Compensation of employees", "Value Added"])
transform!(comp_of_va, ["Compensation of employees", "Wages and salaries"] => ByRow(/) => :l_w)
transform!(comp_of_va,
		   :naics => ByRow(x -> get(bea_oews, string(x), x)),
		   renamecols = false)
select!(comp_of_va, [:year, :naics, :l_w])
# setdiff(oews_industries, resource_cost_factors[!,:naics])
CSV.write(joinpath("data", "comp_of_va.csv"), comp_of_va)
