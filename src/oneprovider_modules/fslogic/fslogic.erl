%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements worker_plugin_behaviour to provide
%% functionality of file system logic.
%% This module shall provide only entry-points for file system logic implementation.
%% @end
%% ===================================================================

-module(fslogic).
-behaviour(worker_plugin_behaviour).

-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("fuse_messages_pb.hrl").
-include("communication_protocol_pb.hrl").
-include("registered_names.hrl").
-include("cluster_elements/request_dispatcher/gsi_handler.hrl").
-include_lib("ctool/include/logging.hrl").

%% ====================================================================
%% API
%% ====================================================================
-export([init/1, handle/2, cleanup/0, fslogic_runner/4, handle_fuse_message/1]).
-export([extract_logical_path/1]).

%% ====================================================================
%% API functions
%% ====================================================================

%% init/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback init/1
-spec init(Args :: term()) -> list().
%% ====================================================================
init(_Args) ->
    Pid = self(),
    {ok, CleaningInterval} = application:get_env(?APP_Name, fslogic_cleaning_period),
    erlang:send_after(CleaningInterval * 1000, Pid, {timer, {asynch, 1, {delete_old_descriptors, Pid}}}),
    {ok, FilesSizeUpdateInterval} = application:get_env(?APP_Name, user_files_size_view_update_period),
    erlang:send_after(FilesSizeUpdateInterval * 1000, Pid, {timer, {asynch, 1, {update_user_files_size_view, Pid}}}),

    % Create acl permission cache
    PermissionCacheProcFun = fun
        (_ProtocolVersion, {grant_permission, StorageFileName, GRUID, Permission}, CacheName) ->
            ets:insert(CacheName, {{fslogic_path:ensure_path_begins_with_slash(StorageFileName), GRUID, Permission}, true}),
            ok;
        (_ProtocolVersion, {has_permission, StorageFileName, GRUID, Permission}, CacheName) ->
            case ets:lookup(CacheName, {fslogic_path:ensure_path_begins_with_slash(StorageFileName), GRUID, Permission}) of
                [{_,true}] -> {ok, true};
                _ -> {ok, false}
            end;
        (_ProtocolVersion, {invalidate_cache, StorageFileName}, CacheName) ->
            ets:match_delete(CacheName,{{fslogic_path:ensure_path_begins_with_slash(StorageFileName), '_', '_'}, '_'})
    end,
    PermissionCacheMapFun = fun
        ({_, StorageFileName, _, _}) ->
            lists:foldl(fun(Char, Sum) -> 10 * Sum + Char end, 0, StorageFileName);
        ({_, StorageFileName}) ->
            lists:foldl(fun(Char, Sum) -> 10 * Sum + Char end, 0, StorageFileName)
    end,

    % Create avilable blocks cache & dao proxy
    RemoteLocationProxyProcFun = fslogic_available_blocks:registered_requests(),
    RemoteLocationProxyMapFun = fun
        ({save_available_blocks, #db_document{record = #available_blocks{file_id = FileId}}}) ->
            lists:foldl(fun(Char, Sum) -> 10 * Sum + Char end, 0, FileId);
        ({save_available_blocks, #available_blocks{file_id = FileId}}) ->
            lists:foldl(fun(Char, Sum) -> 10 * Sum + Char end, 0, FileId);
        ({file_truncated, _, FileId, _, _, _, _}) ->
            lists:foldl(fun(Char, Sum) -> 10 * Sum + Char end, 0, FileId);
        ({file_synchronized, _, FileId, _, _}) ->
            lists:foldl(fun(Char, Sum) -> 10 * Sum + Char end, 0, FileId);
        ({external_available_blocks_changed, _, FileId, _}) ->
            lists:foldl(fun(Char, Sum) -> 10 * Sum + Char end, 0, FileId);
        ({file_block_modified, _, FileId, _, _, _, _, _}) ->
            lists:foldl(fun(Char, Sum) -> 10 * Sum + Char end, 0, FileId);
        ({_, FileId}) ->
            lists:foldl(fun(Char, Sum) -> 10 * Sum + Char end, 0, FileId)
    end,

    % generate process lists
    SubProcList = worker_host:generate_sub_proc_list([
        {pemission_cache, ?CACHE_TREE_MAX_DEPTH, ?CACHE_TREE_MAX_WIDTH, PermissionCacheProcFun, PermissionCacheMapFun, simple},
        {available_blocks_dao_proxy, ?CACHE_TREE_MAX_DEPTH, ?CACHE_TREE_MAX_WIDTH, RemoteLocationProxyProcFun, RemoteLocationProxyMapFun, simple}
    ]),

    % register map functions for process trees
    RequestMap = fun
        ({grant_permission, _, _, _}) -> pemission_cache;
        ({has_permission, _, _, _}) -> pemission_cache;
        ({invalidate_cache, _}) -> pemission_cache;
        ({save_available_blocks, _}) -> available_blocks_dao_proxy;
        ({list_all_available_blocks, _}) -> available_blocks_dao_proxy;
        ({get_file_size, _}) -> available_blocks_dao_proxy;
        ({get_available_blocks, _}) -> available_blocks_dao_proxy;
        ({invalidate_blocks_cache, _}) -> available_blocks_dao_proxy;
        ({file_truncated, _, _, _, _, _, _}) -> available_blocks_dao_proxy;
        ({file_synchronized, _, _, _, _}) -> available_blocks_dao_proxy;
        ({external_available_blocks_changed, _, _, _}) -> available_blocks_dao_proxy;
        ({file_block_modified, _, _, _, _, _, _, _}) -> available_blocks_dao_proxy;
        (_) -> non
    end,
    DispMapFun = fun
        ({save_available_blocks, #db_document{record = #available_blocks{file_id = FileId}}}) ->
            lists:foldl(fun(Char, Sum) -> 2 * Sum + Char end, 0, FileId);
        ({save_available_blocks, #available_blocks{file_id = FileId}}) ->
            lists:foldl(fun(Char, Sum) -> 2 * Sum + Char end, 0, FileId);
        ({get_available_blocks, FileId}) ->
            lists:foldl(fun(Char, Sum) -> 2 * Sum + Char end, 0, FileId);
        ({list_all_available_blocks, FileId}) ->
            lists:foldl(fun(Char, Sum) -> 2 * Sum + Char end, 0, FileId);
        ({invalidate_blocks_cache, FileId}) ->
            lists:foldl(fun(Char, Sum) -> 2 * Sum + Char end, 0, FileId);
        ({file_truncated, _, FileId, _, _, _, _}) ->
            lists:foldl(fun(Char, Sum) -> 2 * Sum + Char end, 0, FileId);
        ({file_synchronized, _, FileId, _, _}) ->
            lists:foldl(fun(Char, Sum) -> 2 * Sum + Char end, 0, FileId);
        ({external_available_blocks_changed, _, FileId, _}) ->
            lists:foldl(fun(Char, Sum) -> 2 * Sum + Char end, 0, FileId);
        ({file_block_modified, _, FileId, _, _, _, _, _}) ->
            lists:foldl(fun(Char, Sum) -> 2 * Sum + Char end, 0, FileId);
        ({get_file_size, FileId}) ->
            lists:foldl(fun(Char, Sum) -> 2 * Sum + Char end, 0, FileId);
        ({invalidate_cache, StorageFileName}) ->
            lists:foldl(fun(Char, Sum) -> 2 * Sum + Char end, 0, StorageFileName);
        ({_, StorageFileName, _, _}) ->
            lists:foldl(fun(Char, Sum) -> 2 * Sum + Char end, 0, StorageFileName);
        (_) -> non
    end,

    ets:new(?fslogic_attr_events_state, [public, named_table, set]),

    #initial_host_description{request_map = RequestMap, dispatcher_request_map = DispMapFun, sub_procs = SubProcList, plug_in_state = ok}.

%% handle/2
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback handle/1. <br/>
%% Processes standard worker requests (e.g. ping) and requests from FUSE.
%% @end
-spec handle(ProtocolVersion :: term(), Request :: term()) -> Result when
  Result :: term().
%% ====================================================================
handle(_ProtocolVersion, ping) ->
  pong;

handle(_ProtocolVersion, healthcheck) ->
    ok;

handle(_ProtocolVersion, get_version) ->
    node_manager:check_vsn();

handle(_ProtocolVersion, {internal_event, EventType, EventArgs}) ->
    fslogic_events:handle_event(EventType, EventArgs);

handle(_ProtocolVersion, {internal_event_handle, Method, Args}) ->
    erlang:apply(fslogic_events, Method, Args);

%% this handler is intended to be called by newly connected clients
%% TODO: create generic mechanism for getting configuration on client startup
handle(ProtocolVersion, is_write_enabled) ->
  try
    case fslogic_objects:get_user() of
      {ok, UserDoc} ->
        case user_logic:get_quota(UserDoc) of
          {ok, #quota{exceeded = Exceeded}} when is_boolean(Exceeded) ->
            %% we can simply return not(Exceeded) but if quota had been exceeded then user deleted file and for some reason
            %% there was no event handler for rm_event then it would need manual trigger to enable writing
            %% in most cases Exceeded == true so in most cases we will not call user_logic:quota_exceeded
            case Exceeded of
              true -> not(user_logic:quota_exceeded(UserDoc, ProtocolVersion));
              _ -> true
            end;
          Error ->
            ?warning("cannot get quota doc for user with dn: ~p, Error: ~p", [fslogic_context:get_user_id(), Error]),
            false
        end;
      Error ->
        ?warning("cannot get user with dn: ~p, Error: ~p", [fslogic_context:get_user_id(), Error]),
        false
    end
  catch
    E1:E2 ->
      ?warning("Error in is_write_enabled handler, Error: ~p:~p", [E1, E2]),
      false
  end;


%% For tests
handle(ProtocolVersion, {delete_old_descriptors_test, Time}) ->
  handle_test(ProtocolVersion, {delete_old_descriptors_test, Time});

handle(ProtocolVersion, {update_user_files_size_view, Pid}) ->
  ?debug("Updating user file sizes for pid: ~p", [Pid]),
  fslogic_meta:update_user_files_size_view(ProtocolVersion),
  {ok, Interval} = application:get_env(?APP_Name, user_files_size_view_update_period),
  erlang:send_after(Interval * 1000, Pid, {timer, {asynch, 1, {update_user_files_size_view, Pid}}}),
  ok;

handle(_ProtocolVersion, {answer_test_message, FuseID, Message}) ->
  request_dispatcher:send_to_fuse(FuseID, #testchannelanswer{message = Message}, "fuse_messages"),
  ok;

handle(ProtocolVersion, {delete_old_descriptors, Pid}) ->
  {Megaseconds,Seconds, _Microseconds} = os:timestamp(),
  Time = 1000000*Megaseconds + Seconds - 15,
  fslogic_objects:delete_old_descriptors(ProtocolVersion, Time),
  {ok, Interval} = application:get_env(?APP_Name, fslogic_cleaning_period),
  erlang:send_after(Interval * 1000, Pid, {timer, {asynch, ProtocolVersion, {delete_old_descriptors, Pid}}}),
  ok;

handle(ProtocolVersion, {getfilelocation_uuid, UUID}) ->
  fslogic_context:set_fuse_id(?CLUSTER_FUSE_ID),
  fslogic_context:set_protocol_version(ProtocolVersion),
  fslogic_runner(fun handle_custom_request/1, getfilelocation_by_uuid, {getfilelocation, UUID});

handle(ProtocolVersion, {getfileattr, UUID}) ->
  fslogic_context:set_protocol_version(ProtocolVersion),
  fslogic_runner(fun handle_custom_request/1, getfileattr_by_uuid, {getfileattr, UUID});


handle(ProtocolVersion, Record) when is_record(Record, fusemessage) ->
    RequestBody = Record#fusemessage.input,
    RequestType = element(1, RequestBody),

    %% Setup context
    fslogic_context:set_fuse_id(get(fuse_id)),
    fslogic_context:set_protocol_version(ProtocolVersion),

    fslogic_runner(fun maybe_handle_fuse_message/1, RequestType, RequestBody);

handle(ProtocolVersion, {internal_call, Record}) ->
    fslogic_context:set_fuse_id(?CLUSTER_FUSE_ID),
    handle(ProtocolVersion, #fusemessage{input = Record, message_type = atom_to_list(utils:record_type(Record))});

handle(_ProtocolVersion, Record) when is_record(Record, callback) ->
  ?debug("Callback request handled: ~p", [Record]),
  Answer = case Record#callback.action of
    channelregistration ->
      try
        gen_server:call({global, ?CCM}, {addCallback, Record#callback.fuse, Record#callback.node, Record#callback.pid}, 1000)
      catch
        E1:E2 ->
          ?error("Callback request ~p error: ~p", [Record, {E1, E2}]),
          error
      end;
    channelclose ->
      try
        gen_server:call({global, ?CCM}, {delete_callback, Record#callback.fuse, Record#callback.node, Record#callback.pid}, 1000)
      catch
        E3:E4 ->
          ?error("Callback request ~p error: ~p", [Record, {E3, E4}]),
          error
      end
  end,
  #atom{value = atom_to_list(Answer)};

%% Handle requests that have wrong structure.
handle(_ProtocolVersion, _Msg) ->
  ?warning("Wrong request: ~p", [_Msg]),
  wrong_request.


%% maybe_handle_fuse_message/1
%% ====================================================================
%% @doc Tries to handle fuse message locally (i.e. handle_fuse_message/1) or delegate request to 'provider_proxy' module.
%% @end
-spec maybe_handle_fuse_message(RequestBody :: tuple()) -> Result :: term().
%% ====================================================================
maybe_handle_fuse_message(RequestBody) ->
    PathCtx = extract_logical_path(RequestBody),
    {ok, AbsolutePathCtx} = fslogic_path:get_full_file_name(PathCtx, utils:record_type(RequestBody)),
    {ok, #space_info{name = SpaceName, providers = Providers} = SpaceInfo} = fslogic_utils:get_space_info_for_path(AbsolutePathCtx),

    Self = cluster_manager_lib:get_provider_id(),

    ?debug("Space for request: ~p, providers: ~p (current ~p). AccessToken: ~p, ~p, FullName: ~p / ~p",
        [SpaceName, Providers, Self, fslogic_context:get_gr_auth(), RequestBody, PathCtx, AbsolutePathCtx]),

    case lists:member(Self, Providers) of
        true ->
            handle_fuse_message(RequestBody);
        false ->
            PrePostProcessResponse = try
                case fslogic_remote:prerouting(SpaceInfo, RequestBody, Providers) of
                    {ok, {reroute, Self, RequestBody1}} ->  %% Request should be handled locally for some reason
                        {ok, handle_fuse_message(RequestBody1)};
                    {ok, {reroute, RerouteToProvider, RequestBody1}} ->
                        RemoteResponse = provider_proxy:reroute_pull_message(RerouteToProvider, fslogic_context:get_gr_auth(),
                            fslogic_context:get_fuse_id(), #fusemessage{input = RequestBody1, message_type = atom_to_list(element(1, RequestBody))}),
                        {ok, RemoteResponse};
                    {ok, {response, Response}} -> %% Do not handle this request and return custom response
                        {ok, Response};
                    {error, PreRouteError} ->
                        ?error("Cannot initialize reouting for request ~p due to error in prerouting handler: ~p", [RequestBody, PreRouteError]),
                        throw({unable_to_reroute_message, {prerouting_error, PreRouteError}})
                end
            catch
                Type:Reason ->
                    ?error_stacktrace("Unable to process remote fslogic request due to: ~p", [{Type, Reason}]),
                    {error, {Type, Reason}}
            end,
            case fslogic_remote:postrouting(SpaceInfo, PrePostProcessResponse, RequestBody) of
                undefined -> throw({unable_to_reroute_message, PrePostProcessResponse});
                LocalResponse -> LocalResponse
            end
    end.

%% handle_test/2
%% ====================================================================
%% @doc Handles calls used during tests
-spec handle_test(ProtocolVersion :: term(), Request :: term()) -> Result when
  Result :: atom().
%% ====================================================================
-ifdef(TEST).
handle_test(ProtocolVersion, {delete_old_descriptors_test, Time}) ->
  fslogic_objects:delete_old_descriptors(ProtocolVersion, Time),
  ok.
-else.
handle_test(_ProtocolVersion, _Request) ->
  not_supported_in_normal_mode.
-endif.

%% cleanup/0
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback cleanup/0
-spec cleanup() -> ok.
%% ====================================================================
cleanup() ->
  ok.

%% ====================================================================
%% Internal functions
%% ====================================================================


%% fslogic_runner/3
%% ====================================================================
%% @doc Runs Method(RequestBody) while catching errors and translating them with
%%      fslogic_errors module.
-spec fslogic_runner(Method :: function(), RequestType :: atom(), RequestBody :: term()) -> Response :: term().
%% ====================================================================
fslogic_runner(Method, RequestType, RequestBody) when is_function(Method) ->
    fslogic_runner(Method, RequestType, RequestBody, fslogic_errors).

%% fslogic_runner/4
%% ====================================================================
%% @doc Runs Method(RequestBody) while catching errors and translating them with
%%      given ErrorHandler module. ErrorHandler module has to export at least gen_error_message/2 (see fslogic_errors:gen_error_message/1).
-spec fslogic_runner(Method :: function(), RequestType :: atom(), RequestBody :: term(), ErrorHandler :: atom()) -> Response :: term().
%% ====================================================================
fslogic_runner(Method, RequestType, RequestBody, ErrorHandler) when is_function(Method) ->
    try
        ?debug("Processing request (type ~p): ~p", [RequestType, RequestBody]),
        Method(RequestBody)
    catch
        Reason ->
            {ErrorCode, ErrorDetails} = fslogic_errors:gen_error_code(Reason),
            %% Manually thrown error, normal interrupt case.
            ?debug_stacktrace("Cannot process request ~p due to error: ~p (code: ~p)", [RequestBody, ErrorDetails, ErrorCode]),
            ErrorHandler:gen_error_message(RequestType, fslogic_errors:normalize_error_code(ErrorCode));
        error:{badmatch, Reason} ->
            {ErrorCode, ErrorDetails} = fslogic_errors:gen_error_code(Reason),
            %% Bad Match assertion - something went wrong, but it could be expected.
            ?warning_stacktrace("Cannot process request ~p due to error: ~p (code: ~p)", [RequestBody, ErrorDetails, ErrorCode]),
            ErrorHandler:gen_error_message(RequestType, fslogic_errors:normalize_error_code(ErrorCode));
        error:{case_clause, Reason} ->
            {ErrorCode, ErrorDetails} = fslogic_errors:gen_error_code(Reason),
            %% Bad Match assertion - something went seriously wrong and we should know about it.
            ?error_stacktrace("Cannot process request ~p due to error: ~p (code: ~p)", [RequestBody, ErrorDetails, ErrorCode]),
            ErrorHandler:gen_error_message(RequestType, fslogic_errors:normalize_error_code(ErrorCode));
        error:UnkError ->
            {ErrorCode, ErrorDetails} = {?VEREMOTEIO, UnkError},
            %% Bad Match assertion - something went horribly wrong. This should not happen.
            ?error_stacktrace("Cannot process request ~p due to unknown error: ~p (code: ~p)", [RequestBody, ErrorDetails, ErrorCode]),
            ErrorHandler:gen_error_message(RequestType, fslogic_errors:normalize_error_code(ErrorCode))
    end.

%% handle_fuse_message/1
%% ====================================================================
%% @doc Processes requests from FUSE.
%% @end
-spec handle_fuse_message(Record :: tuple()) -> Result when
  Result :: term().
%% ====================================================================
handle_fuse_message(Req = #updatetimes{file_logic_name = FName, atime = ATime, mtime = MTime, ctime = CTime}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_generic:update_times(FullFileName, ATime, MTime, CTime);

handle_fuse_message(Req = #changefileowner{file_logic_name = FName, uid = UID}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_generic:change_file_owner(FullFileName, UID);

handle_fuse_message(Req = #changefilegroup{file_logic_name = FName, gid = GID, gname = GName}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_generic:change_file_group(FullFileName, GID, GName);

handle_fuse_message(Req = #changefileperms{file_logic_name = FName, perms = Perms}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_generic:change_file_perms(FullFileName, Perms);

handle_fuse_message(Req = #checkfileperms{file_logic_name = FName, type = Type}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_generic:check_file_perms(FullFileName, Type);

handle_fuse_message(Req = #getfileattr{file_logic_name = FName}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_generic:get_file_attr(FullFileName);

handle_fuse_message(Req = #getxattr{file_logic_name = FName, name = Name}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_generic:get_xattr(FullFileName, Name);

handle_fuse_message(Req = #setxattr{file_logic_name = FName, name = Name, value = Value, flags = Flags}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_generic:set_xattr(FullFileName, Name, Value, Flags);

handle_fuse_message(Req = #removexattr{file_logic_name = FName, name = Name}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_generic:remove_xattr(FullFileName,Name);

handle_fuse_message(Req = #listxattr{file_logic_name = FName}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_generic:list_xattr(FullFileName);

handle_fuse_message(Req = #getacl{file_logic_name = FName}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_generic:get_acl(FullFileName);

handle_fuse_message(Req = #setacl{file_logic_name = FName, entities = Entities}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_generic:set_acl(FullFileName, Entities);

handle_fuse_message(Req = #getfileuuid{file_logic_name = FName}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_utility:get_file_uuid(FullFileName);

handle_fuse_message(Req = #getfilelocation{file_logic_name = FName, open_mode = OpenMode, force_cluster_proxy = ForceClusterProxy}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_regular:get_file_location(FullFileName, OpenMode, ForceClusterProxy);

handle_fuse_message(Req = #getnewfilelocation{file_logic_name = FName, mode = Mode, force_cluster_proxy = ForceClusterProxy}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_regular:get_new_file_location(FullFileName, Mode, ForceClusterProxy);

handle_fuse_message(Req = #synchronizefileblock{logical_name = FName, offset = Offset, size = Size}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_available_blocks:synchronize_file_block(FullFileName, Offset, Size);

% @todo Remove FUSE ID from protocol and get it from context
handle_fuse_message(Req = #fileblockmodified{logical_name = FName, fuse_id = FuseId, sequence_number = SequenceNumber, offset = Offset, size = Size}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_available_blocks:file_block_modified(FullFileName, FuseId, SequenceNumber, Offset, Size);

% @todo Remove FUSE ID from protocol and get it from context
handle_fuse_message(Req = #filetruncated{logical_name = FName, fuse_id = FuseId, sequence_number = SequenceNumber, size = Size}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_available_blocks:file_truncated(FullFileName, FuseId, SequenceNumber, Size);

handle_fuse_message(Req = #getfileblockmap{logical_name = FName}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_generic:get_file_block_map(FullFileName);

handle_fuse_message(Req = #requestfileblock{logical_name = FName, offset = Offset, size = Size}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_available_blocks:synchronize_file_block(FullFileName, Offset, Size);

handle_fuse_message(Req = #createfileack{file_logic_name = FName}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_regular:create_file_ack(FullFileName);

handle_fuse_message(Req = #filenotused{file_logic_name = FName}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_regular:file_not_used(FullFileName);

handle_fuse_message(Req = #renewfilelocation{file_logic_name = FName}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_regular:renew_file_location(FullFileName);

handle_fuse_message(Req = #createdir{dir_logic_name = FName, mode = Mode}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_special:create_dir(FullFileName, Mode);

handle_fuse_message(Req = #getfilechildren{dir_logic_name = FName, offset = Offset, children_num = Count}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    {ok, UserPathTokens} = fslogic_path:verify_file_name(FName),
    fslogic_req_special:get_file_children(FullFileName, UserPathTokens, Offset, Count);

handle_fuse_message(Req = #getfilechildrencount{dir_logic_name = FName}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_special:get_file_children_count(FullFileName);

handle_fuse_message(Req = #deletefile{file_logic_name = FName}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_generic:delete_file(FullFileName);

handle_fuse_message(Req = #renamefile{from_file_logic_name = FromFName, to_file_logic_name = ToFName}) ->
  {ok, FullFileName} = fslogic_path:get_full_file_name(FromFName, utils:record_type(Req)),
  {ok, FullNewFileName} = fslogic_path:get_full_file_name(ToFName, utils:record_type(Req)),
  fslogic_req_generic:rename_file(FullFileName, FullNewFileName);

%% Symbolic link creation. From - link name, To - path pointed by new link
handle_fuse_message(Req = #createlink{from_file_logic_name = FName, to_file_logic_name = LinkValue}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_special:create_link(FullFileName, LinkValue);

%% Fetch link data (target path)
handle_fuse_message(Req = #getlink{file_logic_name = FName}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, utils:record_type(Req)),
    fslogic_req_special:get_link(FullFileName);

handle_fuse_message(_Req = #getstatfs{}) ->
    fslogic_req_generic:get_statfs();

%% Storage requests
handle_fuse_message(_Req = #createstoragetestfilerequest{storage_id = StorageId}) ->
    fslogic_req_storage:create_storage_test_file(StorageId);

handle_fuse_message(_Req = #storagetestfilemodifiedrequest{storage_id = StorageId, relative_path = RelPath, text = Text}) ->
    fslogic_req_storage:storage_test_file_modified(StorageId, RelPath, Text);

handle_fuse_message(_Req = #clientstorageinfo{storage_info = SInfo}) ->
    fslogic_req_storage:client_storage_info(SInfo);

handle_fuse_message(#attrunsubscribe{file_uuid = FileUUID}) ->
    fslogic_req_generic:attr_unsubscribe(FileUUID);

%% Test message
handle_fuse_message(_Req = #testchannel{answer_delay_in_ms = Interval, answer_message = Answer}) ->
    timer:apply_after(Interval, gen_server, cast, [?MODULE, {asynch, fslogic_context:get_protocol_version(), {answer_test_message, fslogic_context:get_fuse_id(), Answer}}]),
    #atom{value = "ok"}.

%% Custom internal request handlers
handle_custom_request({getfileattr, UUID}) ->
    {ok, FileDoc} = fslogic_objects:get_file({uuid, UUID}),
    fslogic_req_generic:get_file_attr(FileDoc);

handle_custom_request({getfilelocation, UUID}) ->
    {ok, FileDoc} = dao_lib:apply(dao_vfs, get_file, [{uuid, UUID}], fslogic_context:get_protocol_version()),
    fslogic_req_regular:get_file_location(FileDoc, ?UNSPECIFIED_MODE).


%% %% extract_logical_path/1
%% %% ====================================================================
%% %% @doc Convinience method that returns logical file path for the operation.
%% %% @end
%% -spec extract_logical_path(Record :: tuple()) -> string() | no_return().
%% %% ====================================================================
extract_logical_path(#getfileattr{file_logic_name = Path}) ->
    Path;
extract_logical_path(#getfileuuid{file_logic_name = Path}) ->
    Path;
extract_logical_path(#getxattr{file_logic_name = Path}) ->
    Path;
extract_logical_path(#setxattr{file_logic_name = Path}) ->
    Path;
extract_logical_path(#removexattr{file_logic_name = Path}) ->
    Path;
extract_logical_path(#listxattr{file_logic_name = Path}) ->
    Path;
extract_logical_path(#getacl{file_logic_name = Path}) ->
    Path;
extract_logical_path(#setacl{file_logic_name = Path}) ->
    Path;
extract_logical_path(#getfilelocation{file_logic_name = Path}) ->
    Path;
extract_logical_path(#deletefile{file_logic_name = Path}) ->
    Path;
extract_logical_path(#renamefile{from_file_logic_name = Path}) ->
    Path;
extract_logical_path(#getnewfilelocation{file_logic_name = Path}) ->
    Path;
extract_logical_path(#requestfileblock{logical_name = Path}) ->
    Path;
extract_logical_path(#synchronizefileblock{logical_name = Path}) ->
    Path;
extract_logical_path(#fileblockmodified{logical_name = Path}) ->
    Path;
extract_logical_path(#filetruncated{logical_name = Path}) ->
    Path;
extract_logical_path(#getfileblockmap{logical_name = Path}) ->
    Path;
extract_logical_path(#filenotused{file_logic_name = Path}) ->
    Path;
extract_logical_path(#renewfilelocation{file_logic_name = Path}) ->
    Path;
extract_logical_path(#getfilechildrencount{dir_logic_name = Path}) ->
    Path;
extract_logical_path(#getfilechildren{dir_logic_name = Path}) ->
    Path;
extract_logical_path(#createdir{dir_logic_name = Path}) ->
    Path;
extract_logical_path(#getlink{file_logic_name = Path}) ->
    Path;
extract_logical_path(#createlink{from_file_logic_name = Path}) ->
    Path;
extract_logical_path(#changefileowner{file_logic_name = Path}) ->
    Path;
extract_logical_path(#changefilegroup{file_logic_name = Path}) ->
    Path;
extract_logical_path(#changefileperms{file_logic_name = Path}) ->
    Path;
extract_logical_path(#checkfileperms{file_logic_name = Path}) ->
    Path;
extract_logical_path(#updatetimes{file_logic_name = Path}) ->
    Path;
extract_logical_path(#createfileack{file_logic_name = Path}) ->
    Path;
extract_logical_path(#attrunsubscribe{file_uuid = UUID}) ->
    {ok, Path} = logical_files_manager:get_file_full_name_by_uuid(UUID),
    Path;
extract_logical_path(_) ->
    "/".
