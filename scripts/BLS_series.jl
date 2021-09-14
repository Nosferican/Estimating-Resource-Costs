using HTTP: HTTP, URI, request
using JSON3: JSON3
using DataFrames: DataFrames, DataFrame, combine, groupby, ByRow, select!, transform!, innerjoin
using Statistics: Statistics, mean
using CSV: CSV

const API_BLS = get(ENV, "api_bls_token", "")

response = request("POST",
                   URI(scheme = "https",
                       host = "api.bls.gov",
                       path = "/publicAPI/v2/timeseries/data",
                       query = ["registrationkey" => API_BLS,
                                "startyear" => 2008,
                                "endyear" => 2019,
                                "annualaverage" => true,
                                "seriesid" => "CMU2020000120000D,CMU2030000120000D"]),
                   ["Content-Type" => "application/x-www-form-urlencoded"])
json = JSON3.read(response.body)
function parse_series(series)
    id = series.seriesID
    data = DataFrame((year = elem.year, value = parse(Float64, elem.value)) for elem in series.data)
    data = combine(groupby(data, :year), :value => mean => id)
end
x = parse_series(json.Results.series[1])
y = parse_series(json.Results.series[2])
z = innerjoin(x, y, on = :year)
transform!(z,
           [:CMU2020000120000D, :CMU2030000120000D] => ByRow((sw, tb) -> (sw + tb) / sw) =>
           :salary_to_total_compensation)
sort!(select!(z, [:year, :salary_to_total_compensation]))
CSV.write(joinpath("data", "bls_salary_wages_to_total_compensation.csv"), z)
