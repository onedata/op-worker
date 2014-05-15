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

-include("veil_modules/dao/dao.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("fuse_messages_pb.hrl").
-include("communication_protocol_pb.hrl").
-include("registered_names.hrl").
-include("logging.hrl").

%% ====================================================================
%% API
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).

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
  {ok, CleaningInterval} = application:get_env(veil_cluster_node, fslogic_cleaning_period),
  erlang:send_after(CleaningInterval * 1000, Pid, {timer, {asynch, 1, {delete_old_descriptors, Pid}}}),
  {ok, FilesSizeUpdateInterval} = application:get_env(veil_cluster_node, user_files_size_view_update_period),
  erlang:send_after(FilesSizeUpdateInterval * 1000, Pid, {timer, {asynch, 1, {update_user_files_size_view, Pid}}}),
  [].

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

%% this handler is intended to be called by newly connected clients
%% TODO: create generic mechanism for getting configuration on client startup
handle(ProtocolVersion, is_write_enabled) ->
  try
    case user_logic:get_user({dn, fslogic_context:get_user_dn()}) of
      {ok, UserDoc} ->
        case user_logic:get_quota(UserDoc) of
          {ok, #quota{exceeded = Exceeded}} when is_boolean(Exceeded) ->
            %% we can simply return not(Exceeded) but if quota had been exceeded then user deleted file and for some reason
            %% there was no event handler for rm_event then it would need manual trigger to enable writing
            %% in most cases Exceeded == true so in most cases we will not call user_logic:quota_exceeded
            case Exceeded of
              true -> not(user_logic:quota_exceeded({dn, fslogic_context:get_user_dn()}, ProtocolVersion));
              _ -> true
            end;
          Error ->
            ?warning("cannot get quota doc for user with dn: ~p, Error: ~p", [fslogic_context:get_user_dn(), Error]),
            false
        end;
      Error ->
        ?warning("cannot get user with dn: ~p, Error: ~p", [fslogic_context:get_user_dn(), Error]),
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
  fslogic_meta:update_user_files_size_view(ProtocolVersion),
  {ok, Interval} = application:get_env(veil_cluster_node, user_files_size_view_update_period),
  erlang:send_after(Interval * 1000, Pid, {timer, {asynch, 1, {update_user_files_size_view, Pid}}}),
  ok;

handle(_ProtocolVersion, {answer_test_message, FuseID, Message}) ->
  request_dispatcher:send_to_fuse(FuseID, #testchannelanswer{message = Message}, "fuse_messages"),
  ok;

handle(ProtocolVersion, {delete_old_descriptors, Pid}) ->
  {Megaseconds,Seconds, _Microseconds} = os:timestamp(),
  Time = 1000000*Megaseconds + Seconds - 15,
  fslogic_objects:delete_old_descriptors(ProtocolVersion, Time),
  {ok, Interval} = application:get_env(veil_cluster_node, fslogic_cleaning_period),
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

    fslogic_runner(fun handle_fuse_message/1, RequestType, RequestBody);

handle(ProtocolVersion, {internal_call, Record}) ->
    fslogic_context:set_fuse_id(?CLUSTER_FUSE_ID),
    handle(ProtocolVersion, #fusemessage{input = Record, message_type = atom_to_list(vcn_utils:record_type(Record))});

handle(_ProtocolVersion, Record) when is_record(Record, callback) ->
  Answer = case Record#callback.action of
    channelregistration ->
      try
        gen_server:call({global, ?CCM}, {addCallback, Record#callback.fuse, Record#callback.node, Record#callback.pid}, 1000)
      catch
        _:_ ->
          error
      end;
    channelclose ->
      try
        gen_server:call({global, ?CCM}, {delete_callback, Record#callback.fuse, Record#callback.node, Record#callback.pid}, 1000)
      catch
        _:_ ->
          error
      end
  end,
  #atom{value = atom_to_list(Answer)};

%% Handle requests that have wrong structure.
handle(_ProtocolVersion, _Msg) ->
  wrong_request.


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

fslogic_runner(Method, RequestType, RequestBody) when is_function(Method) ->
    try
        ?debug("Processing request (type ~p): ~p", [RequestType, RequestBody]),
        Method(RequestBody)
    catch
        Reason ->
            {ErrorCode, ErrorDetails} = fslogic_errors:gen_error_code(Reason),
            %% Manually thrown error, normal interrupt case.
            ?debug_stacktrace("Cannot process request ~p due to error: ~p (code: ~p)", [RequestBody, ErrorDetails, ErrorCode]),
            fslogic_errors:gen_error_message(RequestType, fslogic_errors:normalize_error_code(ErrorCode));
        error:{badmatch, {error, Reason}} ->
            {ErrorCode, ErrorDetails} = fslogic_errors:gen_error_code(Reason),
            %% Bad Match assertion - something went wrong, but it could be expected.
            ?warning("Cannot process request ~p due to error: ~p (code: ~p)", [RequestBody, ErrorDetails, ErrorCode]),
            ?debug_stacktrace("Cannot process request ~p due to error: ~p (code: ~p)", [RequestBody, ErrorDetails, ErrorCode]),
            fslogic_errors:gen_error_message(RequestType, fslogic_errors:normalize_error_code(ErrorCode));
        error:{case_clause, {error, Reason}} ->
            {ErrorCode, ErrorDetails} = fslogic_errors:gen_error_code(Reason),
            %% Bad Match assertion - something went seriously wrong and we should know about it.
            ?error_stacktrace("Cannot process request ~p due to error: ~p (code: ~p)", [RequestBody, ErrorDetails, ErrorCode]),
            fslogic_errors:gen_error_message(RequestType, fslogic_errors:normalize_error_code(ErrorCode));
        error:UnkError ->
            {ErrorCode, ErrorDetails} = {?VEREMOTEIO, UnkError},
            %% Bad Match assertion - something went horribly wrong. This should not happen.
            ?error_stacktrace("Cannot process request ~p due to unknown error: ~p (code: ~p)", [RequestBody, ErrorDetails, ErrorCode]),
            fslogic_errors:gen_error_message(RequestType, fslogic_errors:normalize_error_code(ErrorCode))
    end.

%% handle_fuse_message/1
%% ====================================================================
%% @doc Processes requests from FUSE.
%% @end
-spec handle_fuse_message(Record :: tuple()) -> Result when
  Result :: term().
%% ====================================================================
handle_fuse_message(Req = #updatetimes{file_logic_name = FName, atime = ATime, mtime = MTime, ctime = CTime}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, vcn_utils:record_type(Req)),
    fslogic_req_generic:update_times(FullFileName, ATime, MTime, CTime);

handle_fuse_message(Req = #changefileowner{file_logic_name = FName, uid = UID, uname = UName}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, vcn_utils:record_type(Req)),
    fslogic_req_generic:change_file_owner(FullFileName, UID, UName);

handle_fuse_message(Req = #changefilegroup{file_logic_name = FName, gid = GID, gname = GName}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, vcn_utils:record_type(Req)),
    fslogic_req_generic:change_file_group(FullFileName, GID, GName);

handle_fuse_message(Req = #changefileperms{file_logic_name = FName, perms = Perms}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, vcn_utils:record_type(Req)),
    fslogic_req_generic:change_file_perms(FullFileName, Perms);

handle_fuse_message(Req = #getfileattr{file_logic_name = FName}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, vcn_utils:record_type(Req)),
    fslogic_req_generic:get_file_attr(FullFileName);

handle_fuse_message(Req = #getfilelocation{file_logic_name = FName}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, vcn_utils:record_type(Req)),
    fslogic_req_regular:get_file_location(FullFileName);

handle_fuse_message(Req = #getnewfilelocation{file_logic_name = FName, mode = Mode}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, vcn_utils:record_type(Req)),
    fslogic_req_regular:get_new_file_location(FullFileName, Mode);

handle_fuse_message(Req = #createfileack{file_logic_name = FName}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, vcn_utils:record_type(Req)),
    fslogic_req_regular:create_file_ack(FullFileName);

handle_fuse_message(Req = #filenotused{file_logic_name = FName}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, vcn_utils:record_type(Req)),
    fslogic_req_regular:file_not_used(FullFileName);

handle_fuse_message(Req = #renewfilelocation{file_logic_name = FName}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, vcn_utils:record_type(Req)),
    fslogic_req_regular:renew_file_location(FullFileName);

handle_fuse_message(Req = #createdir{dir_logic_name = FName, mode = Mode}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, vcn_utils:record_type(Req)),
    fslogic_req_special:create_dir(FullFileName, Mode);

handle_fuse_message(Req = #getfilechildren{dir_logic_name = FName, offset = Offset, children_num = Count}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, vcn_utils:record_type(Req)),
    fslogic_req_special:get_file_children(FullFileName, Offset, Count);

handle_fuse_message(Req = #deletefile{file_logic_name = FName}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, vcn_utils:record_type(Req)),
    fslogic_req_generic:delete_file(FullFileName);

handle_fuse_message(Req = #renamefile{from_file_logic_name = FromFName, to_file_logic_name = ToFName}) ->
  {ok, FullFileName} = fslogic_path:get_full_file_name(FromFName, vcn_utils:record_type(Req)),
  {ok, FullNewFileName} = fslogic_path:get_full_file_name(ToFName, vcn_utils:record_type(Req)),
  fslogic_req_generic:rename_file(FullFileName, FullNewFileName);

%% Symbolic link creation. From - link name, To - path pointed by new link
handle_fuse_message(Req = #createlink{from_file_logic_name = FName, to_file_logic_name = LinkValue}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, vcn_utils:record_type(Req)),
    fslogic_req_special:create_link(FullFileName, LinkValue);

%% Fetch link data (target path)
handle_fuse_message(Req = #getlink{file_logic_name = FName}) ->
    {ok, FullFileName} = fslogic_path:get_full_file_name(FName, vcn_utils:record_type(Req)),
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
    fslogic_req_regular:get_file_location(FileDoc).

