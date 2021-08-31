# OEWS.jl
#= This script pulls the data from
- U.S. Department of Labor
  - Bureau of Labor Statistics
    - Occupational Employment and Wage Statistics
	  - National industry-specific and by ownership
	    - OCC code
		- NAICS
		- Average Annual Salary
		- Number of Employees

For the private sector.

## Source: https://www.bls.gov/oes/tables.htm
=#
using InfoZIP: InfoZIP, unzip
using Downloads: download
using HTTP: HTTP, URI, request
using ExcelFiles: ExcelFiles, load, openxl
using XLSX: XLSX, readxlsx, readtable, sheetnames
using CSV
using DataFrames

#=
We first download the Occupational Employment and Wage Statistics data files for 2008-2019.
=#  
for yr in 08:19
	yr = lpad(yr, 2, '0')
	isfile(joinpath("data", "oews_$yr.zip")) ||
		download(string(URI(scheme = "https",
                            host = "www.bls.gov",
                            path = "/oes/special.requests/oesm$(yr)in4.zip")),
				 joinpath("data", "oews_$yr.zip"))
end
#=
Unzip 2008-2011
=#
for yr in 08:11
	yr = lpad(yr, 2, '0')
	isdir(joinpath("data", "oesm$(yr)in4")) || mkdir(joinpath("data", "oesm$(yr)in4"))
    isempty(readdir(joinpath("data", "oesm$(yr)in4"))) &&
        unzip(joinpath("data", "oews_$(yr).zip"), joinpath("data", "oesm$(yr)in4"))
end
#=
Unzip 2012-2019
=#
for yr in 12:19
    isdir(joinpath("data", "oews_$(yr).zip")) ||
	    unzip(joinpath("data", "oews_$(yr).zip"), "data")
end
#=
Download the crosswalk for OCC codes from 2010 - 2019.
=#
if !isfile(joinpath("data", "oes_2019_hybrid_structure.xlsx"))
	download(string(URI(scheme = "https",
                        host = "www.bls.gov",
                        path = "/oes/oes_2019_hybrid_structure.xlsx")),
			 joinpath("data", "oes_2019_hybrid_structure.xlsx"))
end
#=
Download the crosswalk for SOC 2000 - 2010 for 2008-2009.
=#
if !isfile(joinpath("data", "soc_2000_to_2010_crosswalk.xls"))
	download(string(URI(scheme = "https",
                        host = "www.bls.gov",
                        path = "/soc/soc_2000_to_2010_crosswalk.xls")),
			 joinpath("data", "soc_2000_to_2010_crosswalk.xls"))
end
#=
Values might be available or not. This helper function handles that logic.
=#
get_the_number(x) = get_the_number(string(x))
get_the_number(x::Number) = x
get_the_number(x::AbstractString) =
    occursin(r"^\d+\.?\d+$", x) ? convert(Int, parse(Float64, x)) : missing

"""
    parse_oews(file::AbstractString)::DataFrame

Returns the parsed data from an OEWS file.
"""
function parse_oews(file::AbstractString)
	if endswith(file, "xls")
		data = DataFrame(load(file, only(openxl(file).workbook.sheet_names())))
	elseif endswith(file, "xlsx")
		data = readxlsx(file)
        data = DataFrame(readtable(file, first(sheetnames(data)))...)
	end
	rename!(data, names(data) .=> lowercase.(names(data)))
	data[!,:year] .= parse(Int, match(r"\d{4}", file).match)
	select!(data, [:year, :occ_code, :occ_title, :naics, :tot_emp, :a_mean])
	transform!(data,
			   :a_mean => ByRow(get_the_number),
			   :tot_emp => ByRow(get_the_number),
			   renamecols = false)
end

data = DataFrame(year = Int[],
                 occ_code = String[],
                 occ_title = String[],
                 naics = String[],
                 tot_emp = Union{Missing,Int}[],
                 a_mean = Union{Missing,Int}[])
# empty!(data)
for dir in filter!(dir -> occursin(r"^oesm\d{2}in4$", dir), readdir("data"))
	for (root, dirs, files) in walkdir(joinpath("data", dir))
		yr = match(r"\d{2}", dir).match
		for file in filter!(file -> occursin("natsector_M20$(yr)_dl", file), files)
            occursin(r"\$", file) && continue # Skip opened Windows file
			println(joinpath(root, file))
			append!(data, parse_oews(joinpath(root, file)))
		end
	end
end
unique!(data)
#=
Sometimes there will be detailed data by ownership. We pick the broadest one.
This is done by picking the record with the highest number of employees reported.
=#
data = combine(groupby(data, [:year, :occ_code, :naics])) do subdf
	first(sort(subdf, order(:tot_emp, rev = true)))
end
#=
We care about the Software Developers and Software Quality Assurance Analysts and Testers occupation.
This is a hybrid OCC code that started being used in 2019 (15-1256).
We will reconstruct the values for it using the crosswalk.
=#
crosswalk = CSV.read(
    joinpath("data", "Software Developers and Software Quality Assurance Analysts and Testers.tsv"),
    DataFrame)
#=
For 2008 - 2009 we need to use the 2000-2010 SOC crosswalk.
=#
soc00_10 = DataFrame(load(joinpath("data", "soc_2000_to_2010_crosswalk.xls"),
						  "sort by SOC 2000"))
select!(soc00_10, [1, 3])
rename!(soc00_10, [:soc00, :soc10])
dropmissing!(soc00_10)
subset!(soc00_10, :soc10 => ByRow(∈(unique(crosswalk[!,:occ10]))))
soc00_10 = Dict(soc00_10[!,:soc10] .=> soc00_10[!,:soc00])
transform!(crosswalk, :occ10 => ByRow(x -> get(soc00_10, x, x)) => :occ00)
#=
Just to confirm the 2019 vintage did not include the detailed occ18 codes we verify it below.
=#
# select!(subset(data, :occ_code => ByRow(∈(unique(vec(Matrix(crosswalk)))))), [:year, :occ_code]) |>
#     unique! |>
#     sort!
#=
Since it is a single OCC code we can filter the records and aggregate easily.
=#
subset!(data, :occ_code => ByRow(∈(unique(vec(Matrix(crosswalk))))))
#=
In case there are missing values, we can omit those and compute the mean based on the available data. 
=#
dropmissing!(data)
#=
We must compute the tot_emp and a_mean.
=#
data = transform(data, [:tot_emp, :a_mean] => ByRow(*) => :salary_and_wages)
select!(data, [:year, :naics, :tot_emp, :salary_and_wages])
data = combine(groupby(data, [:year, :naics]),
               [:tot_emp, :salary_and_wages] .=> sum,
               renamecols = false)
data = transform(data, [:salary_and_wages, :tot_emp] => ByRow(/) => :a_mean)
select!(data, [:year, :naics, :tot_emp, :a_mean])

CSV.write(joinpath("data", "oews_15-1256.csv"), data)
