using HTTP: HTTP, request, URI
using JSON3: JSON3
using XLSX: XLSX, readxlsx, readtable, readdata, sheetnames
using CSV: CSV
using DataFrames: DataFrames, DataFrame, select!, unstack,
                  subset!, ByRow
const BEA_API_KEY = get(ENV, "api_bea_token", "")

#=
Find the mapping to of industry to NAICS2 (sector) through the APIs
=#
url = URI(scheme = "https", host = "apps.bea.gov", path = "/api/data",
          query = ["UserID" => BEA_API_KEY,
                   "method" => "GetData",
                   "DatasetName" => "InputOutput",
                   "TableID" => 259,
                   "Year" => join(2008:2019, ','),
                   "ResultFormat" => "JSON"
                  ])
response = request("GET", url)
@assert response.status == 200
json = JSON3.read(response.body)
x = DataFrame((year = row.Year, descr = row.RowDescr, value = parse(Int, row.DataValue))
                for row in json.BEAAPI.Results.Data
                  if (row.RowDescr ∈ ["Total Intermediate",
                                      "Compensation of employees",
                                      "Gross operating surplus",
                                      "Other taxes on production",
                                      ""]) &
                     isequal("5415", row.ColCode))
y = unstack(x, :descr, :value)
transform!(y,
           ["Total Intermediate", "Compensation of employees"] => ByRow(/) => :ti_coe,
           ["Gross operating surplus", "Compensation of employees"] => ByRow(/) => :gos_coe,
           ["Other taxes on production", "Compensation of employees"] => ByRow(/) => :top_coe)
select!(y, [:year, :ti_coe, :gos_coe, :top_coe])
CSV.write(joinpath("data", "usetbl5415.csv"), y)
