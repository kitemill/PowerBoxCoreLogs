module PowerBoxCoreLogs

export powerbox_read, list, lookup, powerbox_expand_file_series,
to_dataframe, @powerbox_to_struct


using DataFrames
using StringEncodings
using TimeZones
using Dates
using UUIDs


OSLO = TimeZone("Europe/Oslo", TimeZones.Class(:LEGACY))


struct FieldId
  sensor::Int
  item::Int
end


struct Field
  sensor::Int
  item::Int
  name::String
  item_name::String
  unit::String
  decimal_digits::Int
end


struct FieldAndData
  field::Field
  data::Union{Nothing,DataFrame}
end


FieldsWithData = Dict{FieldId,FieldAndData}


function id(f::Field)::FieldId
  FieldId(f.sensor, f.item)
end


function powerbox_read(filename::String; timezone::TimeZone = OSLO, limit::Number = Inf)
  fields_and_data = FieldsWithData()
  time_seconds_base::Int64 = 0

  function timezone_to_utc_epoch(epoch_oslo_ms)
    tmp = ZonedDateTime(unix2datetime(epoch_oslo_ms .* 0.001), timezone)
    Int64(datetime2unix(DateTime(tmp, UTC)) .* 1_000.0 |> round)
  end

  function past_limit()::Bool
    maximum(vcat(0, [isnothing(x.data) ? 0 : length(x.data.v) for x = values(fields_and_data)])) >= limit
  end

  for next_filename = powerbox_expand_file_series(filename)
    if past_limit(); break; end

    open(next_filename, "r") do fobj
      for line_ in eachline(fobj)
        if past_limit(); break; end

        line = decode(Vector{UInt8}(line_), "latin1")
        if !isnothing(match(r"^(\d\d\d);", line))
          # #SensorsTable section
          fields = split(line, ";")
          if length(fields) >= 6
            sensor = parse(Int, fields[1])
            item = parse(Int, fields[2])
            name = fields[3]
            item_name = fields[4]
            unit = fields[5]
            decimal_digits = parse(Int, fields[6])
            field = Field(sensor, item, name, item_name, unit, decimal_digits)
            fields_and_data[id(field)] = FieldAndData(field, nothing)
          end
          continue
        end

        m0 = match(r"^#Time=0x([0-9A-F]+)$", line)
        if !isnothing(m0)
          time_seconds_base = parse(Int, m0[1], base = 16)
          continue
        end

        if !isnothing(match(r"^:\d\d;\d\d\d;\d\d;", line))
          # parsing a data point
          fields = split(line, ";")

          hundreds = parse(Int, fields[1][2:end])
          sensor = parse(Int, fields[2])
          item = parse(Int, fields[3])


          if length(fields) > 8
            # number items
            # items given as consecutively increasing sensor item ids
            for (i, v) = enumerate(fields[5:end])
              if !isnothing(match(r"^0x", v))
                key = FieldId(sensor, item + i - 1)
                if haskey(fields_and_data, key)
                  f_d = fields_and_data[key]
                  if isnothing(f_d.data)
                    if f_d.field.decimal_digits > 0
                      f_d = FieldAndData(f_d.field, DataFrame(:_time => Vector{Int64}(), :v => Vector{Float64}()))
                    else
                      f_d = FieldAndData(f_d.field, DataFrame(:_time => Vector{Int64}(), :v => Vector{Int}()))
                    end
                    fields_and_data[key] = f_d
                  end

                  if f_d.field.decimal_digits > 0
                    push!(f_d.data, (timezone_to_utc_epoch(time_seconds_base * 1000 + hundreds * 10), parse(Int64, v) * 10.0^-(f_d.field.decimal_digits)))
                  else
                    push!(f_d.data, (timezone_to_utc_epoch(time_seconds_base * 1000 + hundreds * 10), parse(Int64, v)))
                  end
                end
              end
            end
          elseif length(fields) == 8
            # text item
            key = FieldId(sensor, item)
            if haskey(fields_and_data, key)
              f_d = fields_and_data[key]
              if isnothing(f_d.data)
                f_d = FieldAndData(f_d.field, DataFrame(:_time => Vector{Int64}(), :v => Vector{String}()))
                fields_and_data[key] = f_d
              end
              push!(f_d.data, (timezone_to_utc_epoch(time_seconds_base * 1000 + hundreds * 10), fields[6]))
            end
          end
          continue
        end
      end
    end
  end
  fields_and_data
end


function powerbox_list_helper(fields_and_data::FieldsWithData)
  sort([("$(x.field.name)/$(x.field.item_name)", x) for x = values(fields_and_data)])
end



function list(fields_and_data::FieldsWithData)::Vector{String}
  [n for (n, _) = powerbox_list_helper(fields_and_data)]
end


function lookup(fields_and_data::FieldsWithData, listed_name::String)::Union{Nothing,DataFrame}
  tmp = powerbox_list_helper(fields_and_data)
  i = findfirst(x -> x[1] == listed_name, tmp)
  isnothing(i) ? nothing : tmp[i][2].data
end


function powerbox_expand_file_series(file0::String)
  result = []
  f = file0
  while isfile(f)
    push!(result, f)
    m = match(r"(.*)_(\d\d)_(Tele.log)", f)
    if isnothing(m)
      break
    end
    f = "$(m[1])_$(lpad(parse(Int, m[2]) + 1, 2, "0"))_$(m[3])"
  end

  result
end


function string_to_struct_safe(s::String)::String
  s1 = filter(isascii, s) |> lowercase
  s2 = replace(s1, r"[^0-9A-Za-z._/ ]" => "")
  s3 = replace(s2, r"[._/ ]" => "_")
  s4 = replace(s3, r"___*" => "_")
  s5 = replace(s4, r"_$" => "")
  s5
end

function to_dataframe(powerbox::Dict{FieldId, FieldAndData}; missing_value = missing)::DataFrame

  function retime(time, data, new_time)
    dt = length(time) >= 2 ? maximum(diff(time)) : 0
    tmp::Vector{Any} = fill(missing, size(new_time))
    j = 1
    for i = 1:length(new_time)
      if j > length(time)
        if (i > 1) && new_time[i] <= time[end] + dt
          tmp[i] = tmp[i - 1]
        end
      elseif new_time[i] >= time[j]
        tmp[i] = data[j]
        j = j + 1
      elseif i > 1
        tmp[i] = tmp[i - 1]
      end
    end
    tmp
  end

  namer = d -> "$(string_to_struct_safe(d.field.name))__$(string_to_struct_safe(d.field.item_name))"
  somedatas = sort([v for (_, v) = powerbox if !isnothing(v.data)], by = namer)
  times = sort(collect(reduce((acc1, x) -> reduce((acc2, y) -> push!(acc1, y), x.data._time, init = acc1), somedatas, init = Set{Int64}())))
  name_and_retimed = [(namer(d), retime(d.data._time, d.data.v, times)) for d = somedatas]
  DataFrame(["_time" => times, [n => r for (n, r) = name_and_retimed]...])
end


macro powerbox_to_struct(filename0, timezone = TimeZone("Europe/Oslo", TimeZones.Class(:LEGACY)))
  f_d = powerbox_read(filename0, limit = 100)

  sensors = unique(sort([k.sensor for (k, _) = f_d]))
  sensor_struct_names = Dict([(s, Symbol("_PowerBoxToStruct_$(string(UUIDs.uuid1())[(end-7):end])")) for s = sensors])
  sensor_names = Dict([(k.sensor, v.field.name) for (k, v) = f_d]) # trick, multiple keys overwritten in Dict
  make_items = sensor -> [k.item for (k, v) = f_d if k.sensor == sensor && !isnothing(v.data) && length(v.data.v) > 0]

  function make_sensor_struct(sensor)
    items = make_items(sensor)

    esc(quote
          struct $(Symbol(sensor_struct_names[sensor]))
            $([Symbol(string_to_struct_safe(f_d[FieldId(sensor, i)].field.item_name)) for i = items]...)
          end

          Base.show(io::IO, powerbox::$(sensor_struct_names[sensor])) = print(io, "[PowerBox Sensor] $(join(fieldnames(typeof(powerbox)), ", "))")
        end)
  end
  
  top_level_struct = Symbol("__PowerBoxToStruct_$(string(UUIDs.uuid1())[(end-7):end])")

  top_level_struct_code = esc(quote
                                struct $(top_level_struct)
                                  _powerbox
                                  $([Symbol(string_to_struct_safe(sensor_names[x])) for x = sensors]...)
                                end

                                Base.show(io::IO, powerbox::$(top_level_struct)) = print(io, "[PowerBox] $(join(fieldnames(typeof(powerbox)), ", "))")
                              end)

  function make_field_id(x)
    :(tmp0[PowerBoxCoreLogs.FieldId($(x.field.sensor), $(x.field.item))].data)
  end

  function make_instance_for_sensor(number, sensor)
    items = make_items(sensor)
    esc(:( $(Symbol("tmp$(number)")) = $(sensor_struct_names[sensor])($([make_field_id(f_d[FieldId(sensor, i)]) for i = items]...)) ))
  end

  quote
    ($(vcat(map(make_sensor_struct, sensors), top_level_struct_code)...))
    let
      $(esc(:(tmp0 = powerbox_read($(filename0), timezone = $(timezone)))))
      $([make_instance_for_sensor(i, n) for (i, n) = enumerate(sensors)]...)
      $(esc(:($(top_level_struct))))($(esc(:(tmp0))), $([esc(Symbol("tmp$(i)")) for (i, _) = enumerate(sensors)]...))
    end
  end
end


end
