using Base: Float64
using HTTP: HTTP, request, URI
using JSON3: JSON3, propertynames
using XLSX: XLSX, readxlsx, readtable, readdata, sheetnames
using CSV: CSV, Rows
using DataFrames: DataFrames, DataFrame, select!, combine, groupby, ByRow, dropmissing!,
                  transform!, rename!
const BEA_API_KEY = get(ENV, "api_bea_token", "")

#=
Find the mapping to of industry to NAICS2 (sector) through the APIs
=#
url = URI(scheme = "https", host = "apps.bea.gov", path = "/api/data",
          query = ["UserID" => BEA_API_KEY,
                   "method" => "GetData",
                   "DatasetName" => "GDPbyIndustry",
                   "TableID" => 6,
                   "Year" => join(2008:2019, ','),
                   "Frequency" => "A",
                   "Industry" => "ALL",
                   "ResultFormat" => "JSON"
                  ])
url = URI(scheme = "https", host = "apps.bea.gov", path = "/api/data",
          query = ["UserID" => BEA_API_KEY,
                   "method" => "GetParameterValues",
                   "DatasetName" => "GDPbyIndustry",
                   "ParameterName" => "TableID",
                   "ResultFormat" => "JSON"
                  ])
url = URI(scheme = "https", host = "apps.bea.gov", path = "/api/data",
          query = ["UserID" => BEA_API_KEY,
                   "method" => "GetData",
                   "DatasetName" => "GDPbyIndustry",
                   "TableID" => 25,
                   "Year" => join(2008:2019, ','),
                   "Frequency" => "A",
                   "Industry" => "ALL",
                   "ResultFormat" => "JSON"
                  ])
response = request("GET", url)
@assert response.status == 200
json = JSON3.read(response.body)
json.BEAAPI.Results.Parameter
json.BEAAPI.Results.ParamValue
json.BEAAPI.Results.Dataset
json.BEAAPI.Results.Error
x = DataFrame(json.BEAAPI.Results[1].Data)
select!(x, [:Year, :Industry, :IndustrYDescription, :DataValue])

sort!(combine(groupby(x, :IndustrYDescription), nrow), order(:nrow, rev = true))[1:8,:]

vars = ["Year", "Industry",
        "Compensation of employees", "Energy inputs",
        "Gross operating surplus", "Intermediate inputs", "Materials inputs",
        "Purchased-services inputs", "Taxes on production and imports less subsidies",
        "Value added"]
subset!(x, :IndustrYDescription => ByRow(∈(vars)))
transform!(x, :DataValue => ByRow(x -> parse(Float64, x)), renamecols = false)
y = combine(groupby(x, [:Year, :Industry])) do subdf
    subdf = unstack(subdf, :IndustrYDescription, :DataValue)
    for col in setdiff(vars, names(subdf))
        subdf[!,col] .= 0
    end
    select!(subdf, vars)
end
dropmissing!(y)
select!(subset(y, "Taxes on production and imports less subsidies" => ByRow(<(0))),
        ["Year", "Industry", "Taxes on production and imports less subsidies"])
println(string(names(y)))
transform!(y,
           ["Taxes on production and imports less subsidies", "Compensation of employees"] =>
                ByRow(/) => :top_coe,
           ["Intermediate inputs", "Compensation of employees"] => ByRow(/) => :ii_coe,
           ["Gross operating surplus", "Compensation of employees"] => ByRow(/) => :gos_coe,
           ["Energy inputs", "Purchased-services inputs", "Compensation of employees"] =>
                ByRow((ene, psi, coe) -> (ene + psi) / coe) => :enepsi_coe)
rename!(y, :Year => :year, :Industry => :naics)
select!(y, [:year, :naics, :top_coe, :ii_coe, :gos_coe, :enepsi_coe])
CSV.write(joinpath("data", "klems.csv"), y)
