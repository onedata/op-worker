%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% CDMI internal functions
%%% @end
%%%-------------------------------------------------------------------
-module(cdmi_internal).
-author("Katarzyna Such").

-include("http/cdmi.hrl").
-include("cdmi_test.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("onenv_test_utils.hrl").

%% API
-export([
    get_file_content/2, object_exists/2, do_request/4, do_request/5,
    create_new_file/2, write_to_file/4, open_file/3,
    mock_opening_file_without_perms/1, unmock_opening_file_without_perms/1,
    get_json_metadata/2, get_xattrs/2, get_acl/2, set_acl/3, get_random_string/0
%%    create_test_dir_and_file/1,
]).


do_request(Node, RestSubpath, Method, Headers) ->
    do_request(Node, RestSubpath, Method, Headers, []).

do_request([_ | _] = Nodes, RestSubpath, get, Headers, Body) ->
    [FRes | _] = Responses = lists:filtermap(fun(Node) ->
        case make_request(Node, RestSubpath, get, Headers, Body) of
            space_not_supported -> false;
            Result -> {true, Result}
        end
    end, Nodes),
    FRes;

do_request([_ | _] = Nodes, RestSubpath, Method, Headers, Body) ->
    lists:foldl(fun
        (Node, space_not_supported) ->
            make_request(Node, RestSubpath, Method, Headers, Body);
        (_Node, Result) ->
            Result
    end, space_not_supported, lists_utils:shuffle(Nodes));

do_request(Node, RestSubpath, Method, Headers, Body) when is_atom(Node) ->
    make_request(Node, RestSubpath, Method, Headers, Body).


%% @private
make_request(Node, RestSubpath, Method, Headers, Body) ->
    case cdmi_test_utils:do_request(Node, RestSubpath, Method, Headers, Body) of
        {ok, RespCode, _RespHeaders, RespBody} = Result ->
            case is_space_supported(Node, RestSubpath) of
                true ->
                    Result;
                false ->
                    % Returned error may not be necessarily ?ERROR_SPACE_NOT_SUPPORTED(_, _)
                    % as some errors may be thrown even before file path resolution attempt
                    % (and such errors are explicitly checked by some tests),
                    % but it should never be any successful response
                    ?assert(RespCode >= 300),
                    case {RespCode, try_to_decode(RespBody)} of
                        {?HTTP_400_BAD_REQUEST, #{<<"error">> :=  #{
                            <<"id">> := <<"spaceNotSupportedBy">>
                        }}}->
                            space_not_supported;
                        _ ->
                            Result
                    end
            end;
        {error, _} = Error ->
            Error
    end.


%% @private
is_space_supported(_Node, "") ->
    true;
is_space_supported(_Node, "/") ->
    true;
is_space_supported(Node, CdmiPath) ->
    {ok, SuppSpaces} = rpc:call(Node, provider_logic, get_spaces, []),
    SpecialObjectIds = [?ROOT_CAPABILITY_ID, ?CONTAINER_CAPABILITY_ID, ?DATAOBJECT_CAPABILITY_ID],

    case binary:split(list_to_binary(CdmiPath), <<"/">>, [global, trim_all]) of
        [<<"cdmi_capabilities">> | _] ->
            true;
        [<<"cdmi_objectid">>, ObjectId | _] ->
            case lists:member(ObjectId, SpecialObjectIds) of
                true ->
                    true;
                false ->
                    {ok, FileGuid} = file_id:objectid_to_guid(ObjectId),
                    SpaceId = file_id:guid_to_space_id(FileGuid),
                    SpaceId == <<"rootDirVirtualSpaceId">> orelse lists:member(SpaceId, SuppSpaces)
            end;
        [SpaceName | _] ->
            lists:any(fun(SpaceId) -> get_space_name(Node, SpaceId) == SpaceName end, SuppSpaces)
    end.


%% @private
get_space_name(Node, SpaceId) ->
    {ok, SpaceName} = rpc:call(Node, space_logic, get_name, [<<"0">>, SpaceId]),
    SpaceName.


%% @private
try_to_decode(Body) ->
    try
        remove_times_metadata(json_utils:decode(Body))
    catch _:invalid_json ->
        Body
    end.


%% @private
remove_times_metadata(ResponseJSON) ->
    Metadata = maps:get(<<"metadata">>, ResponseJSON, undefined),
    case Metadata of
        undefined -> ResponseJSON;
        _ -> Metadata1 = maps:without( [<<"cdmi_ctime">>,
            <<"cdmi_atime">>,
            <<"cdmi_mtime">>], Metadata),
            maps:put(<<"metadata">>, Metadata1, ResponseJSON)
    end.


%%create_test_dir_and_file(Config) ->
%%    SpaceName = oct_background:get_space_name(Config#cdmi_test_config.space_selector),
%%    TestFileName = get_random_string(),
%%    FullTestDirName = ?build_test_root_path(Config),
%%    FullTestFileName = cdmi_test_utils:build_test_root_path(Config, filename:join(?FUNCTION_NAME, "1")),
%%
%%    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
%%        #dir_spec{
%%            name = atom_to_binary(?FUNCTION_NAME),
%%            children = [
%%                #file_spec{
%%                    name = list_to_binary(TestFileName),
%%                    content = ?FILE_CONTENT
%%                }
%%            ]
%%        }, Config#cdmi_test_config.p1_selector
%%    ),
%%
%%    {binary_to_list(SpaceName), TestDirName, FullTestDirName, TestFileName, FullTestFileName}.


object_exists(Path, Config) ->
    WorkerP1 = oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
    SessionId = oct_background:get_user_session_id(user2, Config#cdmi_test_config.p1_selector),
    case lfm_proxy:stat(WorkerP1, SessionId,
        {path, absolute_binary_path(Path)}) of
        {ok, _} ->
            true;
        {error, ?ENOENT} ->
            false
    end.


create_new_file(Path, Config) ->
    WorkerP1 = oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
    SessionId = oct_background:get_user_session_id(user2, Config#cdmi_test_config.p1_selector),
    lfm_proxy:create(WorkerP1, SessionId, absolute_binary_path(Path)).


open_file(ProviderSelector, Path, OpenMode) ->
    Worker = oct_background:get_random_provider_node(ProviderSelector),
    SessionId = oct_background:get_user_session_id(user2, ProviderSelector),
    lfm_proxy:open(Worker, SessionId, {path, absolute_binary_path(Path)}, OpenMode).


write_to_file(Path, Data, Offset, Config) ->
    WorkerP1 = oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
    {ok, FileHandle} = ?assertMatch({ok, _}, open_file(Config#cdmi_test_config.p1_selector, Path, write), ?ATTEMPTS),
    Result = lfm_proxy:write(WorkerP1, FileHandle, Offset, Data),
    lfm_proxy:close(WorkerP1, FileHandle),
    Result.


get_file_content(Path, Config) ->
    WorkerP2 = oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector),
    case open_file(Config#cdmi_test_config.p2_selector, Path, read) of
        {ok, FileHandle} ->
            Result = case lfm_proxy:check_size_and_read(
                WorkerP2, FileHandle, ?FILE_OFFSET_START, ?FILE_SIZE_INFINITY) of
                {error, Error} -> {error, Error};
                {ok, Content} -> Content
            end,
            lfm_proxy:close(WorkerP2, FileHandle),
            Result;
        {error, Error} -> {error, Error}
    end.


%% @private
absolute_binary_path(Path) ->
    list_to_binary(ensure_begins_with_slash(Path)).


%% @private
ensure_begins_with_slash(Path) ->
    ReversedBinary = list_to_binary(lists:reverse(Path)),
    lists:reverse(binary_to_list(filepath_utils:ensure_ends_with_slash(ReversedBinary))).


mock_opening_file_without_perms(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    test_node_starter:load_modules(Workers, [?MODULE]),
    test_utils:mock_new(Workers, lfm),
    test_utils:mock_expect(
        Workers, lfm, monitored_open, fun(_, _, _) -> {error, ?EACCES} end).


unmock_opening_file_without_perms(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    test_utils:mock_unload(Workers, lfm).


set_acl(Path, Acl, Config) ->
    WorkerP1 = oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
    SessionId = oct_background:get_user_session_id(user2, Config#cdmi_test_config.p1_selector),
    lfm_proxy:set_acl(WorkerP1, SessionId, {path, absolute_binary_path(Path)}, Acl).


get_acl(Path, Config) ->
    WorkerP2 = oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector),
    SessionId = oct_background:get_user_session_id(user2, Config#cdmi_test_config.p2_selector),
    lfm_proxy:get_acl(WorkerP2, SessionId, {path, absolute_binary_path(Path)}).


get_xattrs(Path, Config) ->
    WorkerP2 = oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector),
    SessionId = oct_background:get_user_session_id(user2, Config#cdmi_test_config.p2_selector),
    case lfm_proxy:list_xattr(WorkerP2, SessionId, {path, absolute_binary_path(Path)}, false, true) of
        {ok, Xattrs} ->
            lists:filtermap(
                fun
                    (<<"cdmi_", _/binary>>) ->
                        false;
                    (XattrName) ->
                        {ok, Xattr} = lfm_proxy:get_xattr(
                            WorkerP2, SessionId, {path, absolute_binary_path(Path)}, XattrName
                        ),
                        {true, Xattr}
                end, Xattrs);
        {error, Error} -> {error, Error}
    end.


get_json_metadata(Path, Config) ->
    WorkerP2 = oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector),
    SessionId = oct_background:get_user_session_id(user2, Config#cdmi_test_config.p2_selector),
    {ok, FileGuid} = lfm_proxy:resolve_guid(WorkerP2, SessionId, absolute_binary_path(Path)),
    opt_file_metadata:get_custom_metadata(WorkerP2, SessionId, ?FILE_REF(FileGuid), json, [], false).


get_random_string() ->
    get_random_string(10, "abcdefghijklmnopqrstuvwxyz1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ").

get_random_string(Length, AllowedChars) ->
    lists:foldl(fun(_, Acc) ->
        [lists:nth(rand:uniform(length(AllowedChars)),
            AllowedChars)]
        ++ Acc
    end, [], lists:seq(1, Length)).
