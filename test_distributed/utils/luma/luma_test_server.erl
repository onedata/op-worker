%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% A minimal implementation of the external LUMA feed service used to back
%%% storages with `lumaFeed = external' in tests. It handles the full LUMA feed
%%% HTTP API (all 6 endpoints defined in luma_external_feed.hrl), so the
%%% provider populates its LUMA DB through the real luma_external_feed code
%%% path - no mocks involved.
%%%
%%% The server runs ON the op_worker node (so it is reachable from within the
%%% provider pod) and is bound to the loopback interface; the resulting
%%% endpoint is `http://127.0.0.1:<port>' and should be passed as the
%%% storage's `lumaFeedUrl' (see #external_feed_luma{} in space_setup_utils.hrl).
%%%
%%% Mappings are registered per storage with {@link set_storage_feed_data/3}.
%%% The feed data map follows the response schemas of the LUMA feed API:
%%% #{
%%%     <<"storageUsers">> => #{
%%%         OnedataUserId => #{<<"storageCredentials">> => ..., <<"displayUid">> => ...}
%%%     },
%%%     <<"spacesDefaults">> => #{
%%%         SpaceId => #{
%%%             <<"posix">> => #{<<"uid">> => ..., <<"gid">> => ...},
%%%             <<"display">> => #{<<"uid">> => ..., <<"gid">> => ...}
%%%         }
%%%     },
%%%     <<"onedataUsers">> => #{
%%%         <<"uids">> => #{Uid => OnedataUserMapping},
%%%         <<"aclUsers">> => #{AclUser => OnedataUserMapping}
%%%     },
%%%     <<"onedataGroups">> => #{AclGroup => OnedataGroupMapping}
%%% }
%%% Every section is optional. Entries missing from the feed data are answered
%%% with 404 (mapping not found). Values are served as-is - registering a
%%% malformed entry is the way to test the product's sanitization failure paths.
%%%
%%% NOTE: this module must be added to ?LOAD_MODULES of any suite that uses it,
%%% as the on-node routines are executed on the op_worker node via rpc.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_test_server).
-author("Bartosz Walkowicz").

-include("modules/storage/luma/luma_external_feed.hrl").
-include_lib("inets/include/httpd.hrl").

-record(luma_test_server, {
    httpd_pid :: pid(),
    endpoint :: binary()
}).

-opaque handle() :: #luma_test_server{}.
-export_type([handle/0]).

-define(SERVER_NAME, "luma_test_server").
%% persistent_term key holding a map of #{StorageId => FeedData} served by the feed
-define(FEED_DATA_KEY, {?MODULE, feed_data}).

%% API
-export([start/1, stop/2, stop_all/1]).
-export([set_storage_feed_data/3]).
-export([endpoint/1]).
%% inets httpd callback module (runs on the op_worker node)
-export([do/1]).
%% on-node routines (executed on op_worker via rpc)
-export([
    start_on_node/0, stop_on_node/1, stop_all_on_node/0,
    set_storage_feed_data_on_node/2
]).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec start(oct_background:node_selector()) -> handle().
start(ProviderSelector) ->
    {HttpdPid, Port} = opw_test_rpc:call(ProviderSelector, ?MODULE, start_on_node, []),
    #luma_test_server{
        httpd_pid = HttpdPid,
        endpoint = <<"http://127.0.0.1:", (integer_to_binary(Port))/binary>>
    }.


-spec stop(oct_background:node_selector(), handle()) -> ok.
stop(ProviderSelector, #luma_test_server{httpd_pid = HttpdPid}) ->
    ok = opw_test_rpc:call(ProviderSelector, ?MODULE, stop_on_node, [HttpdPid]).


%% @doc Stops all test luma servers lingering on the node (e.g. left over by
%% previous test cases) and clears the registered feed data.
-spec stop_all(oct_background:node_selector()) -> ok.
stop_all(ProviderSelector) ->
    ok = opw_test_rpc:call(ProviderSelector, ?MODULE, stop_all_on_node, []).


%% @doc Registers the feed data (see the module doc for the schema) served for
%% the given storage, replacing any data registered for it before.
-spec set_storage_feed_data(oct_background:node_selector(), binary(), map()) -> ok.
set_storage_feed_data(ProviderSelector, StorageId, FeedData) ->
    ok = opw_test_rpc:call(
        ProviderSelector, ?MODULE, set_storage_feed_data_on_node, [StorageId, FeedData]
    ).


-spec endpoint(handle()) -> binary().
endpoint(#luma_test_server{endpoint = Endpoint}) ->
    Endpoint.


%%%===================================================================
%%% On-node routines
%%%===================================================================


%% @doc Runs on the op_worker node.
-spec start_on_node() -> {pid(), inet:port_number()}.
start_on_node() ->
    {ok, _} = application:ensure_all_started(inets),
    {ok, HttpdPid} = inets:start(httpd, [
        {port, 0},
        {bind_address, {127, 0, 0, 1}},
        {server_name, ?SERVER_NAME},
        % httpd requires the roots to point at existing dirs, but they are
        % irrelevant - all requests are handled by this module (see do/1)
        {server_root, "/tmp"},
        {document_root, "/tmp"},
        {modules, [?MODULE]}
    ]),
    Port = proplists:get_value(port, httpd:info(HttpdPid, [port])),
    {HttpdPid, Port}.


%% @doc Runs on the op_worker node.
-spec stop_on_node(pid()) -> ok.
stop_on_node(HttpdPid) ->
    ok = inets:stop(httpd, HttpdPid),
    clear_feed_data().


%% @doc Runs on the op_worker node.
-spec stop_all_on_node() -> ok.
stop_all_on_node() ->
    clear_feed_data(),
    case lists:keymember(inets, 1, application:which_applications()) of
        false ->
            ok;
        true ->
            lists:foreach(fun(HttpdPid) ->
                case proplists:get_value(server_name, httpd:info(HttpdPid, [server_name])) of
                    ?SERVER_NAME -> ok = inets:stop(httpd, HttpdPid);
                    _ -> ok
                end
            end, [Pid || {httpd, Pid} <- inets:services()])
    end.


%% @doc Runs on the op_worker node.
-spec set_storage_feed_data_on_node(binary(), map()) -> ok.
set_storage_feed_data_on_node(StorageId, FeedData) ->
    AllFeedData = persistent_term:get(?FEED_DATA_KEY, #{}),
    persistent_term:put(?FEED_DATA_KEY, AllFeedData#{
        StorageId => normalize_uid_keys(FeedData)
    }).


%%%===================================================================
%%% inets httpd callback
%%%===================================================================


%% @doc
%% Runs on the op_worker node, invoked by inets httpd for every request.
%% Resolves the requested mapping in the registered feed data and responds with
%% 200 + the mapping, or 404 + an empty JSON object when there is none (also
%% for unknown endpoints/methods - the product treats 404 as "mapping not found").
%% @end
-spec do(#mod{}) -> {proceed, list()}.
do(#mod{method = "POST", request_uri = RequestUri, entity_body = EntityBody, data = Data}) ->
    ReqBody = json_utils:decode(iolist_to_binary(EntityBody)),
    Response = case maps:find(request_path(RequestUri), endpoint_routing()) of
        {ok, BuildLookupKeys} ->
            AllFeedData = persistent_term:get(?FEED_DATA_KEY, #{}),
            case kv_utils:get(BuildLookupKeys(ReqBody), AllFeedData, undefined) of
                undefined -> {404, #{}};
                Mapping -> {200, Mapping}
            end;
        error ->
            {404, #{}}
    end,
    respond(Response, Data);
do(#mod{data = Data}) ->
    respond({404, #{}}, Data).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
%% @doc Maps each LUMA feed endpoint onto a fun building the feed data lookup
%% keys from the request body. Built at runtime as the endpoint path macros
%% expand to function calls and cannot be used in match patterns.
-spec endpoint_routing() -> #{binary() => fun((json_utils:json_map()) -> [binary()])}.
endpoint_routing() ->
    #{
        ?ONEDATA_USER_TO_CREDENTIALS_PATH => fun(
            #{<<"storageId">> := StorageId, <<"onedataUserId">> := UserId}
        ) ->
            [StorageId, <<"storageUsers">>, UserId]
        end,
        ?DEFAULT_POSIX_CREDENTIALS_PATH => fun(
            #{<<"storageId">> := StorageId, <<"spaceId">> := SpaceId}
        ) ->
            [StorageId, <<"spacesDefaults">>, SpaceId, <<"posix">>]
        end,
        ?DISPLAY_CREDENTIALS_PATH => fun(
            #{<<"storageId">> := StorageId, <<"spaceId">> := SpaceId}
        ) ->
            [StorageId, <<"spacesDefaults">>, SpaceId, <<"display">>]
        end,
        ?UID_TO_ONEDATA_USER_PATH => fun(
            #{<<"storageId">> := StorageId, <<"uid">> := Uid}
        ) ->
            [StorageId, <<"onedataUsers">>, <<"uids">>, integer_to_binary(Uid)]
        end,
        ?ACL_USER_TO_ONEDATA_USER_PATH => fun(
            #{<<"storageId">> := StorageId, <<"aclUser">> := AclUser}
        ) ->
            [StorageId, <<"onedataUsers">>, <<"aclUsers">>, AclUser]
        end,
        ?ACL_GROUP_TO_ONEDATA_GROUP_PATH => fun(
            #{<<"storageId">> := StorageId, <<"aclGroup">> := AclGroup}
        ) ->
            [StorageId, <<"onedataGroups">>, AclGroup]
        end
    }.


%% @private
-spec respond({non_neg_integer(), json_utils:json_term()}, list()) -> {proceed, list()}.
respond({Code, JsonBody}, Data) ->
    Body = binary_to_list(json_utils:encode(JsonBody)),
    {proceed, [{response, {response, [
        {code, Code},
        {content_type, "application/json"},
        {content_length, integer_to_list(length(Body))}
    ], Body}} | Data]}.


%% @private
-spec request_path(string()) -> binary().
request_path(RequestUri) ->
    [PathOnly | _] = string:split(RequestUri, "?"),
    case list_to_binary(PathOnly) of
        <<"/", Path/binary>> -> Path;
        Path -> Path
    end.


%% @private
%% @doc The uid lookup keys are binaries (uids travel as integers in request
%% bodies and are converted on lookup) - convert integer keys of the uids
%% section, which are more natural to write in the feed data literals.
-spec normalize_uid_keys(map()) -> map().
normalize_uid_keys(FeedData = #{<<"onedataUsers">> := OnedataUsers = #{<<"uids">> := Uids}}) ->
    FeedData#{<<"onedataUsers">> => OnedataUsers#{<<"uids">> => maps:fold(fun
        (Uid, Mapping, Acc) when is_integer(Uid) -> Acc#{integer_to_binary(Uid) => Mapping};
        (Uid, Mapping, Acc) -> Acc#{Uid => Mapping}
    end, #{}, Uids)}};
normalize_uid_keys(FeedData) ->
    FeedData.


%% @private
-spec clear_feed_data() -> ok.
clear_feed_data() ->
    _ = persistent_term:erase(?FEED_DATA_KEY),
    ok.
