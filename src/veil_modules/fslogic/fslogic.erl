%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements worker_plugin_behaviour to provide
%% functionality of file system logic.
%% @end
%% ===================================================================

-module(fslogic).
-behaviour(worker_plugin_behaviour).

-include("registered_names.hrl").
-include("veil_modules/dao/dao.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("files_common.hrl").
-include("fuse_messages_pb.hrl").
-include("communication_protocol_pb.hrl").
-include("veil_modules/dao/dao_users.hrl").
-include("veil_modules/dao/dao_types.hrl").
-include("cluster_elements/request_dispatcher/gsi_handler.hrl").
-include("logging.hrl").

%% ====================================================================
%% API
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).
-export([handle_test/2]).

%% ====================================================================
%% Test API
%% ====================================================================
-ifdef(TEST).
%% eunit
-export([handle_fuse_message/1]).
%% ct
-endif.

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
    case user_logic:get_user({dn, get(user_id)}) of
      {ok, UserDoc} ->
        case user_logic:get_quota(UserDoc) of
          {ok, #quota{exceeded = Exceeded}} when is_boolean(Exceeded) ->
            %% we can simply return not(Exceeded) but if quota had been exceeded then user deleted file and for some reason
            %% there was no event handler for rm_event then it would need manual trigger to enable writing
            %% in most cases Exceeded == true so in most cases we will not call user_logic:quota_exceeded
            case Exceeded of
              true -> not(user_logic:quota_exceeded({dn, get(user_id)}, ProtocolVersion));
              _ -> true
            end;
          Error ->
            ?warning("cannot get quota doc for user with dn: ~p, Error: ~p", [get(user_id), Error]),
            false
        end;
      Error ->
        ?warning("cannot get user with dn: ~p, Error: ~p", [get(user_id), Error]),
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
  update_user_files_size_view(ProtocolVersion),
  {ok, Interval} = application:get_env(veil_cluster_node, user_files_size_view_update_period),
  erlang:send_after(Interval * 1000, Pid, {timer, {asynch, 1, {update_user_files_size_view, Pid}}}),
  ok;

handle(_ProtocolVersion, {answer_test_message, FuseID, Message}) ->
  request_dispatcher:send_to_fuse(FuseID, #testchannelanswer{message = Message}, "fuse_messages"),
  ok;

handle(ProtocolVersion, {delete_old_descriptors, Pid}) ->
  {Megaseconds,Seconds, _Microseconds} = os:timestamp(),
  Time = 1000000*Megaseconds + Seconds - 15,
  delete_old_descriptors(ProtocolVersion, Time),
  {ok, Interval} = application:get_env(veil_cluster_node, fslogic_cleaning_period),
  erlang:send_after(Interval * 1000, Pid, {timer, {asynch, ProtocolVersion, {delete_old_descriptors, Pid}}}),
  ok;

handle(ProtocolVersion, {getfilelocation_uuid, UUID}) ->
  {DocFindStatus, FileDoc} = dao_lib:apply(dao_vfs, get_file, [{uuid, UUID}], ProtocolVersion),
  getfilelocation(ProtocolVersion, DocFindStatus, FileDoc, ?CLUSTER_FUSE_ID);

handle(ProtocolVersion, {getfileattr, UUID}) ->
  {ok, FileDoc} = fslogic_objects:get_file({uuid, UUID}),
  fslogic_context:set_protocol_version(ProtocolVersion),
  fslogic_req_generic:get_file_attr(FileDoc);


handle(ProtocolVersion, Record) when is_record(Record, fusemessage) ->
    RequestBody = Record#fusemessage.input,
    RequestType = element(1, RequestBody),
    try
        %% Setup context
        fslogic_context:set_fuse_id(get(fuse_id)),
        fslogic_context:set_protocol_version(ProtocolVersion),
        fslogic_context:set_user_dn(get(user_dn)),

        handle_fuse_message(RequestBody)
    catch
        Reason ->
            {ErrorCode, ErrorDetails} = fslogic_errors:gen_error_code(Reason),
            ?error_stacktrace("Cannot process request ~p due to error: ~p (code: ~p)", [RequestBody, ErrorDetails, ErrorCode]),
            fslogic_errors:gen_error_message(RequestType, fslogic_errors:normalize_error_code(ErrorCode));
        error:{badmatch, {error, Reason}} ->
            {ErrorCode, ErrorDetails} = fslogic_errors:gen_error_code(Reason),
            ?error_stacktrace("Cannot process request ~p due to error: ~p (code: ~p)", [RequestBody, ErrorDetails, ErrorCode]),
            fslogic_errors:gen_error_message(RequestType, fslogic_errors:normalize_error_code(ErrorCode));
        error:{case_clause, {error, Reason}} ->
            {ErrorCode, ErrorDetails} = fslogic_errors:gen_error_code(Reason),
            ?error_stacktrace("Cannot process request ~p due to error: ~p (code: ~p)", [RequestBody, ErrorDetails, ErrorCode]),
            fslogic_errors:gen_error_message(RequestType, fslogic_errors:normalize_error_code(ErrorCode));
        error:UnkError ->
            {ErrorCode, ErrorDetails} = {?VEREMOTEIO, UnkError},
            ?error_stacktrace("Cannot process request ~p due to unknown error: ~p (code: ~p)", [RequestBody, ErrorDetails, ErrorCode]),
            fslogic_errors:gen_error_message(RequestType, fslogic_errors:normalize_error_code(ErrorCode))
    end;

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


%% handle_test/3
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

%% handle_fuse_message/3
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

%% Test message
handle_fuse_message(_Req = #testchannel{answer_delay_in_ms = Interval, answer_message = Answer}) ->
  timer:apply_after(Interval, gen_server, cast, [?MODULE, {asynch, fslogic_context:get_protocol_version(), {answer_test_message, fslogic_context:get_fuse_id(), Answer}}]),
  #atom{value = "ok"};

handle_fuse_message(Record) when is_record(Record, createstoragetestfilerequest) ->
    try
        StorageId = Record#createstoragetestfilerequest.storage_id,
        Length = 20,
        {A, B, C} = now(),
        random:seed(A, B, C),
        Text = list_to_binary(random_ascii_lowercase_sequence(Length)),
        {ok, DeleteStorageTestFileTime} = application:get_env(?APP_Name, delete_storage_test_file_time),
        {ok, #veil_document{record = #user{login = Login}}} = get_user_doc(),
        {ok, #veil_document{record = StorageInfo}} = dao_lib:apply(dao_vfs, get_storage, [{id, StorageId}], ProtocolVersion),
        StorageHelperInfo = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, StorageInfo),
        {ok, Path} = create_storage_test_file(StorageHelperInfo, Login),
        % Delete storage test file after 'delete_storage_test_file_time' seconds
        spawn(fun() ->
            timer:sleep(DeleteStorageTestFileTime * 1000),
            storage_files_manager:delete(StorageHelperInfo, Path)
        end),
        Length = storage_files_manager:write(StorageHelperInfo, Path, Text),
        #createstoragetestfileresponse{answer = true, relative_path = Path, text = Text}
    catch
        _:_ -> #createstoragetestfileresponse{answer = false}
    end;

handle_fuse_message(ProtocolVersion, Record, _FuseID) when is_record(Record, storagetestfilemodifiedrequest) ->
    try
        StorageId = Record#storagetestfilemodifiedrequest.storage_id,
        Path = Record#storagetestfilemodifiedrequest.relative_path,
        Text = Record#storagetestfilemodifiedrequest.text,
        {ok, #veil_document{record = StorageInfo}} = dao_lib:apply(dao_vfs, get_storage, [{id, StorageId}], ProtocolVersion),
        StorageHelperInfo = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, StorageInfo),
        {ok, Bytes} = storage_files_manager:read(StorageHelperInfo, Path, 0, length(Text)),
        Text = binary_to_list(Bytes),
        storage_files_manager:delete(StorageHelperInfo, Path),
        #storagetestfilemodifiedresponse{answer = true}
    catch
        _:_ -> #storagetestfilemodifiedresponse{answer = false}
    end;

handle_fuse_message(ProtocolVersion, Record, FuseId) when is_record(Record, clientstorageinfo) ->
    try
        {ok, #veil_document{record = FuseSession} = FuseSessionDoc} = dao_lib:apply(dao_cluster, get_fuse_session, [FuseId], ProtocolVersion),
        ClientStorageInfo = lists:map(fun({_, StorageId, Root}) ->
            {StorageId, #storage_helper_info{name = "DirectIO", init_args = [Root]}} end, Record#clientstorageinfo.storage_info),
        NewFuseSessionDoc = FuseSessionDoc#veil_document{record = FuseSession#fuse_session{client_storage_info = ClientStorageInfo}},
        {ok, _} = dao_lib:apply(dao_cluster, save_fuse_session, [NewFuseSessionDoc], ProtocolVersion),
        lager:info("Client storage info saved in session."),
        #atom{value = ?VOK}
    catch
        _:_ -> #atom{value = ?VEREMOTEIO}
    end.