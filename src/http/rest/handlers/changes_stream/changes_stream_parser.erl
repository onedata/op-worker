%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Parser for changes stream requests.
%%%
%%% This module is responsible for parsing HTTP requests for changes stream.
%%% It builds #changes_monitoring_spec record based on request parameters and body.
%%%
%%% For detailed documentation about changes stream functionality,
%%% see changes_stream.hrl.
%%% @end
%%%--------------------------------------------------------------------
-module(changes_stream_parser).
-author("Bartosz Walkowicz").

-include("http/changes_stream.hrl").
-include("middleware/middleware.hrl").

%% API
-export([parse_request/1]).

-define(DEFAULT_TIMEOUT, <<"infinity">>).
-define(DEFAULT_LAST_SEQ, <<"now">>).
-define(DEFAULT_ALWAYS, false).


%%%===================================================================
%%% API
%%%===================================================================


-spec parse_request(cowboy_req:req()) ->
    {cowboy_req:req(), changes_stream_processor:changes_monitoring_spec()}.
parse_request(Req) ->
    SpaceId = cowboy_req:binding(sid, Req),
    {Arguments, Req2} = read_arguments(Req),

    ChangesMonitoringSpec = #changes_monitoring_spec{
        space_id = SpaceId,

        timeout = parse_timeout(Arguments),
        start_after_seq = parse_start_at_seq(SpaceId, Arguments),

        triggers = parse_triggers(Arguments),
        doc_monitoring_specs = parse_doc_monitoring_specs(Arguments)
    },

    {Req2, ChangesMonitoringSpec}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec read_arguments(cowboy_req:req()) -> {json_utils:json_map(), cowboy_req:req()}.
read_arguments(Req) ->
    try
        QueryParams = http_parser:parse_query_string(Req),

        {ok, Body, Req2} = cowboy_req:read_body(Req),
        ParsedBody = case Body of
            <<"">> -> #{};
            _ -> json_utils:decode(Body)
        end,

        {maps:merge(ParsedBody, QueryParams), Req2}
    catch _:_ ->
        throw(?ERR_MALFORMED_DATA(?err_ctx()))
    end.


%% @private
-spec parse_timeout(json_utils:json_map()) -> infinity | integer() | no_return().
parse_timeout(Arguments) ->
    case maps:get(<<"timeout">>, Arguments, ?DEFAULT_TIMEOUT) of
        ?DEFAULT_TIMEOUT ->
            infinity;
        NumberBin ->
            parse_integer(<<"timeout">>, NumberBin)
    end.


%% @private
-spec parse_start_at_seq(od_space:id(), json_utils:json_map()) ->
    null | integer() | no_return().
parse_start_at_seq(SpaceId, Arguments) ->
    Key = <<"startAtSeq">>,
    case maps:get(Key, Arguments, maps:get(<<"last_seq">>, Arguments, ?DEFAULT_LAST_SEQ)) of
        ?DEFAULT_LAST_SEQ ->
            dbsync_state:get_seq(SpaceId, oneprovider:get_id());
        NumberBin ->
            parse_integer(Key, NumberBin)
    end.


%% @private
-spec parse_integer(binary(), binary()) -> integer() | no_return().
parse_integer(Param, ValueBin) ->
    try
        binary_to_integer(ValueBin)
    catch _:_ ->
        throw(?ERR_BAD_VALUE_INTEGER(?err_ctx(), Param))
    end.


%% @private
-spec parse_triggers(json_utils:json_map()) -> changes_stream_processor:triggers() | no_return().
parse_triggers(Arguments) ->
    RawTriggers = case maps:get(<<"triggers">>, Arguments, undefined) of
        undefined ->
            ?OBSERVABLE_DOCUMENTS;
        TriggersSpec when is_list(TriggersSpec) ->
            TriggersSpec;
        _ ->
            throw(?ERR_BAD_VALUE_LIST_OF_STRINGS(?err_ctx(), <<"triggers">>))
    end,

    lists:usort(lists:map(fun
        (<<"fileMeta">>) -> file_meta;
        (<<"fileLocation">>) -> file_location;
        (<<"times">>) -> times;
        (<<"customMetadata">>) -> custom_metadata;
        (_) -> throw(?ERR_BAD_VALUE_NOT_ALLOWED(?err_ctx(), <<"triggers">>, ?OBSERVABLE_DOCUMENTS))
    end, RawTriggers)).


%% @private
-spec parse_doc_monitoring_specs(json_utils:json_map()) ->
    [changes_stream_processor:doc_monitoring_spec()] | no_return().
parse_doc_monitoring_specs(Arguments) ->
    DocMonitoringSpecs = lists:foldl(fun(DocName, SpecsAcc) ->
        case maps:get(DocName, Arguments, undefined) of
            undefined ->
                SpecsAcc;
            RawSpec ->
                is_map(RawSpec) orelse throw(?ERR_BAD_VALUE_JSON(?err_ctx(), DocName)),

                [parse_doc_monitoring_spec(DocName, RawSpec) | SpecsAcc]
        end
    end, [], ?OBSERVABLE_DOCUMENTS),

    case DocMonitoringSpecs of
        [] -> throw(?ERR_MISSING_AT_LEAST_ONE_VALUE(?err_ctx(), ?OBSERVABLE_DOCUMENTS));
        _ -> DocMonitoringSpecs
    end.


%% @private
-spec parse_doc_monitoring_spec(binary(), json_utils:json_map()) ->
    changes_stream_processor:doc_monitoring_spec().
parse_doc_monitoring_spec(DocName = <<"fileMeta">>, RawSpec) ->
    #doc_monitoring_spec{
        doc_type = file_meta,
        always_include_in_other_docs_changes = parse_always_flag(DocName, RawSpec),
        observed_fields_for_values = parse_fields(DocName, maps:get(<<"fields">>, RawSpec, []))
    };

parse_doc_monitoring_spec(DocName = <<"fileLocation">>, RawSpec) ->
    #doc_monitoring_spec{
        doc_type = file_location,
        always_include_in_other_docs_changes = parse_always_flag(DocName, RawSpec),
        observed_fields_for_values = parse_fields(DocName, maps:get(<<"fields">>, RawSpec, []))
    };

parse_doc_monitoring_spec(DocName = <<"times">>, RawSpec) ->
    #doc_monitoring_spec{
        doc_type = times,
        always_include_in_other_docs_changes = parse_always_flag(DocName, RawSpec),
        observed_fields_for_values = parse_fields(DocName, maps:get(<<"fields">>, RawSpec, []))
    };

parse_doc_monitoring_spec(DocName = <<"customMetadata">>, RawSpec) ->
    Fields = maps:get(<<"fields">>, RawSpec, []),
    Exists = maps:get(<<"exists">>, RawSpec, []),
    case {is_list(Fields), is_list(Exists)} of
        {true, true} ->
            ok;
        {false, _} ->
            throw(?ERR_BAD_VALUE_LIST_OF_STRINGS(?err_ctx(), <<DocName/binary, ".fields">>));
        {_, false} ->
            throw(?ERR_BAD_VALUE_LIST_OF_STRINGS(?err_ctx(), <<DocName/binary, ".exists">>))
    end,

    #doc_monitoring_spec{
        doc_type = custom_metadata,
        always_include_in_other_docs_changes = parse_always_flag(DocName, RawSpec),
        observed_fields_for_values = Fields,
        observed_fields_for_existence = Exists
    }.

%% @private
-spec parse_always_flag(binary(), json_utils:json_map()) -> true | false | no_return().
parse_always_flag(DocName, Spec) ->
    case maps:get(<<"always">>, Spec, ?DEFAULT_ALWAYS) of
        true -> true;
        false -> false;
        _ -> throw(?ERR_BAD_VALUE_BOOLEAN(?err_ctx(), <<DocName/binary, ".always">>))
    end.


%% @private
-spec parse_fields(binary(), term()) -> [{binary(), integer()}] | no_return().
parse_fields(<<"fileMeta">>, RawFields) when is_list(RawFields) ->
    [{FieldName, file_meta_field_idx(FieldName)} || FieldName <- RawFields];
parse_fields(<<"fileLocation">>, RawFields) when is_list(RawFields) ->
    [{FieldName, file_location_field_idx(FieldName)} || FieldName <- RawFields];
parse_fields(<<"times">>, RawFields) when is_list(RawFields) ->
    [{FieldName, times_field_idx(FieldName)} || FieldName <- RawFields];
parse_fields(DocName, _RawFields) ->
    throw(?ERR_BAD_VALUE_LIST_OF_STRINGS(?err_ctx(), <<DocName/binary, ".fields">>)).


%% @private
-spec file_meta_field_idx(binary()) -> integer() | no_return().
file_meta_field_idx(<<"name">>) -> #file_meta.name;
file_meta_field_idx(<<"type">>) -> #file_meta.type;
file_meta_field_idx(<<"mode">>) -> #file_meta.mode;
file_meta_field_idx(<<"owner">>) -> #file_meta.owner;
file_meta_field_idx(<<"provider_id">>) -> #file_meta.provider_id;
file_meta_field_idx(<<"shares">>) -> #file_meta.shares;
file_meta_field_idx(<<"deleted">>) -> #file_meta.deleted;
file_meta_field_idx(_FieldName) ->
    throw(?ERR_BAD_VALUE_NOT_ALLOWED(?err_ctx(), <<"fileMeta.fields">>, ?OBSERVABLE_FILE_META_FIELDS)).


%% @private
-spec file_location_field_idx(binary()) -> integer() | no_return().
file_location_field_idx(<<"provider_id">>) -> #file_location.provider_id;
file_location_field_idx(<<"storage_id">>) -> #file_location.storage_id;
file_location_field_idx(<<"size">>) -> #file_location.size;
file_location_field_idx(<<"space_id">>) -> #file_location.space_id;
file_location_field_idx(<<"storage_file_created">>) -> #file_location.storage_file_created;
file_location_field_idx(_FieldName) ->
    throw(?ERR_BAD_VALUE_NOT_ALLOWED(?err_ctx(), <<"fileLocation.fields">>, ?OBSERVABLE_FILE_LOCATION_FIELDS)).


%% @private
-spec times_field_idx(binary()) -> integer() | no_return().
times_field_idx(<<"atime">>) -> #times.atime;
times_field_idx(<<"mtime">>) -> #times.mtime;
times_field_idx(<<"ctime">>) -> #times.ctime;
times_field_idx(_FieldName) ->
    throw(?ERR_BAD_VALUE_NOT_ALLOWED(?err_ctx(), <<"times.fields">>, ?OBSERVABLE_TIME_FIELDS)).
