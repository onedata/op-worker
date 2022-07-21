%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module handling sanitization of audit log browse options.
%%% @end
%%%-------------------------------------------------------------------
-module(audit_log_browse_opts).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/errors.hrl").
-include_lib("cluster_worker/include/modules/datastore/infinite_log.hrl").

%% API
-export([sanitize/1]).
-export([json_data_spec/0, from_json/1]).


-define(MAX_LISTING_LIMIT, 1000).
-define(DEFAULT_LISTING_LIMIT, 1000).

-type index() :: json_infinite_log_model:entry_index().
-type timestamp_millis() :: time:millis().
-type offset() :: integer().
-type limit() :: 1..?MAX_LISTING_LIMIT.

-type opts() :: #{
    start_from => undefined | {index, index()} | {timestamp, timestamp_millis()},
    offset => offset(),
    limit => limit(),
    direction => ?BACKWARD | ?FORWARD
}.

-export_type([index/0, timestamp_millis/0, offset/0, limit/0, opts/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec sanitize(json_utils:json_map()) -> opts().
sanitize(Data) ->
    SanitizedData = middleware_sanitizer:sanitize_data(Data, json_data_spec()),
    from_json(SanitizedData).


-spec json_data_spec() -> middleware_sanitizer:data_spec().
json_data_spec() ->
    #{optional => #{
        <<"index">> => {binary, fun(IndexBin) ->
            try
                _ = binary_to_integer(IndexBin),
                true
            catch _:_ ->
                throw(?ERROR_BAD_DATA(<<"index">>, <<"not numerical">>))
            end
        end},
        <<"timestamp">> => {integer, {not_lower_than, 0}},
        <<"offset">> => {integer, any},
        <<"limit">> => {integer, {between, 1, ?MAX_LISTING_LIMIT}},
        <<"direction">> => {atom, [?FORWARD, ?BACKWARD]}
    }}.


-spec from_json(json_utils:json_map()) -> opts().
from_json(Data) ->
    #{
        start_from => infer_start_from(Data),
        offset => maps:get(<<"offset">>, Data, 0),
        limit => maps:get(<<"limit">>, Data, ?DEFAULT_LISTING_LIMIT),
        direction => maps:get(<<"direction">>, Data, ?BACKWARD)
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec infer_start_from(json_utils:json_map()) ->
    undefined | {index, index()} | {timestamp, timestamp_millis()}.
infer_start_from(#{<<"timestamp">> := Timestamp}) -> {timestamp, Timestamp};
infer_start_from(#{<<"index">> := Index}) -> {index, Index};
infer_start_from(_) -> undefined.
