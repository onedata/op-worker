%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module shoul be used as an proxy to helpers_nif module.
%%       This module controls way of accessing helpers_nif methods.
%% @end
%% ===================================================================

-module(helpers).

-include("oneprovider_modules/dao/dao_vfs.hrl").
-include_lib("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").
-include("remote_file_management_pb.hrl").
-include("communication_protocol_pb.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").

-export([exec/2, exec/3, exec/4, exec/5]).
%% ===================================================================
%% API
%% ===================================================================


%% exec/3
%% ====================================================================
%% @doc Executes apply(helper_nif, Method, Args) through slave node. <br/>
%%      Before executing, fields from struct SHInfo are preappend to Args list. <br/>
%%      You can also skip SHInfo argument in order to pass exact Args into target Method.    
%% @end
-spec exec(Method :: atom(), SHInfo :: #storage_helper_info{}, [Arg :: term()]) -> 
    {error, Reason :: term()} | Response when Response :: term().
%% ====================================================================
exec(Method, SHInfo = #storage_helper_info{}, Args) ->
    Args1 = [SHInfo#storage_helper_info.name | [SHInfo#storage_helper_info.init_args | Args]],
    exec(Method, Args1).


%% exec/2
%% ====================================================================
%% @doc Executes apply(helper_nif, Method, Args) through slave node. <br/>
%% @end
-spec exec(Method :: atom(), [Arg :: term()]) ->
    {error, Reason :: term()} | Response when Response :: term().
%% ====================================================================
exec(Method, Args) when is_atom(Method), is_list(Args) ->
    EGroup = case fslogic_context:get_fs_group_ctx() of
                 [MGroup | _] -> MGroup;
                 _            -> -1
             end,
    exec(fslogic_context:get_fs_user_ctx(), EGroup, Method, Args).


%% exec/5
%% ====================================================================
%% @doc Same as exec/3 but allows to set UserName and GroupId for file system permissions check.
%%
%% @end
-spec exec(UserName :: string(), GroupId :: integer(), Method :: atom(), SHInfo :: #storage_helper_info{}, [Arg :: term()]) ->
    {error, Reason :: term()} | Response when Response :: term().
%% ====================================================================
exec(UserName, GroupId, Method, SHInfo = #storage_helper_info{}, Args) ->
    Args1 = [SHInfo#storage_helper_info.name | [SHInfo#storage_helper_info.init_args | Args]],
    exec(UserName, GroupId, Method, Args1).


%% exec/4
%% ====================================================================
%% @doc Same as exec/2 but allows to set UserName and GroupId for file system permissions check.
%%
%% @end
-spec exec(UserName :: string(), GroupId :: integer(), Method :: atom(), [Arg :: term()]) ->
    {error, Reason :: term()} | Response when Response :: term().
%% ====================================================================
exec(UserName, GroupId, Method, Args) when is_atom(Method), is_list(Args) ->
    Args1 = [UserName, GroupId] ++ Args,
    ?debug("Helpers storage CTX ~p ~p", [UserName, GroupId]),
    ?debug("helpers:exec with args: ~p ~p", [Method, Args1]),

    case Args of
        ["ClusterProxy", HelperArgs | MethodArgs] ->
            reroute_to_remote_provider(HelperArgs, Method, MethodArgs);
        _ ->
            case gsi_handler:call(helpers_nif, Method, Args1) of
                {error, 'NIF_not_loaded'} ->
                    ok = load_helpers(),
                    gsi_handler:call(helpers_nif, Method, Args1);
                Other -> Other
            end
    end.


%% load_helpers/0
%% ====================================================================
%% @doc Loads NIF library into slave node. Nodes are started and managed by {@link gsi_handler}
%% @end
-spec load_helpers() -> ok | {error, Reason :: term()}.
%% ====================================================================
load_helpers() ->
    {ok, Prefix} = application:get_env(?APP_Name, nif_prefix),
    case gsi_handler:call(helpers_nif, start, [atom_to_list(Prefix)]) of
        ok -> ok;
        {error,{reload, _}} -> ok;
        {error, Reason} -> 
            ?error("Could not load helpers NIF lib due to error: ~p", [Reason]),
            {error, Reason}
    end.


%% reroute_to_remote_provider/3
%% ====================================================================
%% @doc Based on given helpers arguments, method and its arguments, constructs request to remote_files_manager and sends it to other provider.
%% @end
-spec reroute_to_remote_provider(HelperArgs :: list(), Method :: atom(), Args :: [term()]) -> Answer :: term() | no_return().
%% ====================================================================
reroute_to_remote_provider(_, open, _) ->
    {0, #st_fuse_file_info{}};
reroute_to_remote_provider(_, release, _) ->
    0;
reroute_to_remote_provider(_, chown_name, _) ->
    0;
reroute_to_remote_provider(_, chown, _) ->
    0;
reroute_to_remote_provider(_, is_reg, _) ->
    %% @todo: implement this, possibly by emulating getattr call
    true;
reroute_to_remote_provider(_, is_dir, _) ->
    %% @todo: implement this, possibly by emulating getattr call
    true;
reroute_to_remote_provider(_, get_flag, [Flag]) ->
    helpers_nif:get_flag(Flag);
reroute_to_remote_provider([SpaceId] = _HelperArgs, mknod, [FileId, Mode, _Dev]) ->
    RequestBody = #createfile{file_id = FileId, mode = Mode},
    do_reroute(SpaceId, RequestBody);
reroute_to_remote_provider([SpaceId] = _HelperArgs, getattr, [FileId]) ->
    RequestBody = #getattr{file_id = FileId},
    do_reroute(SpaceId, RequestBody);
reroute_to_remote_provider([SpaceId] = _HelperArgs, unlink, [FileId]) ->
    RequestBody = #deletefileatstorage{file_id = FileId},
    do_reroute(SpaceId, RequestBody);
reroute_to_remote_provider([SpaceId] = _HelperArgs, truncate, [FileId, Size]) ->
    RequestBody = #truncatefile{file_id = FileId, length = Size},
    do_reroute(SpaceId, RequestBody);
reroute_to_remote_provider([SpaceId] = _HelperArgs, read, [FileId, Size, Offset, _FFI]) ->
    RequestBody = #readfile{file_id = FileId, size = Size, offset = Offset},
    do_reroute(SpaceId, RequestBody);
reroute_to_remote_provider([SpaceId] = _HelperArgs, write, [FileId, Buf, Offset, _FFI]) ->
    RequestBody = #writefile{file_id = FileId, data = Buf, offset = Offset},
    do_reroute(SpaceId, RequestBody);
reroute_to_remote_provider([SpaceId] = _HelperArgs, chmod, [FileId, Mode]) ->
    RequestBody = #changepermsatstorage{file_id = FileId, perms = Mode},
    do_reroute(SpaceId, RequestBody);
reroute_to_remote_provider([SpaceId] = _HelperArgs, Method, Args) ->
    ?error("Unsupported local ClusterProxy request ~p(~p) for SpaceId ~p", [Method, Args, SpaceId]),
    throw(unsupported_local_cluster_proxy_req).


%% do_reroute/2
%% ====================================================================
%% @doc Reroute given remote_files_manager's request to other provider.
%% @end
-spec do_reroute(SpaceId :: binary(), RequestBody :: term()) -> Response :: term() | no_return().
%% ====================================================================
do_reroute(SpaceId, RequestBody) ->
    {ok, #space_info{providers = Providers}} = fslogic_objects:get_space({uuid, SpaceId}),

    [RerouteToProvider | _] = Providers,
    ?debug("Helper reroute to: ~p", [RerouteToProvider]),
    try
        Response = provider_proxy:reroute_pull_message(RerouteToProvider, fslogic_context:get_gr_auth(),
            fslogic_context:get_fuse_id(), #remotefilemangement{space_id = SpaceId, input = RequestBody, message_type = atom_to_list(element(1, RequestBody))}),
        cluster_proxy_response_to_internal(Response)
    catch
        Type:Reason ->
            ?error_stacktrace("Unable to process remote files manager request to provider ~p due to: ~p", [RerouteToProvider, {Type, Reason}]),
            throw({unable_to_reroute_message, Reason})
    end.


%% cluster_proxy_response_to_internal/1
%% ====================================================================
%% @doc Translates response from remote_files_manager to format expected by storage_files_manager.
%% @end
-spec cluster_proxy_response_to_internal(Response :: term()) -> InternalReponse :: term() | no_return().
%% ====================================================================
cluster_proxy_response_to_internal(#atom{value = ErrorStatus}) ->
    ErrorCode = fslogic_errors:oneerror_to_posix(fslogic_errors:normalize_error_code(ErrorStatus)),
    -ErrorCode;
cluster_proxy_response_to_internal(#filedata{answer_status = ErrorStatus, data = Data}) ->
    ErrorCode = fslogic_errors:oneerror_to_posix(fslogic_errors:normalize_error_code(ErrorStatus)),
    case ErrorCode of
        0 -> {size(Data), Data};
        _ -> {-ErrorCode, Data}
    end;
cluster_proxy_response_to_internal(#writeinfo{answer_status = ErrorStatus, bytes_written = BytesWritten}) ->
    ErrorCode = fslogic_errors:oneerror_to_posix(fslogic_errors:normalize_error_code(ErrorStatus)),
    case ErrorCode of
        0 -> BytesWritten;
        _ -> -ErrorCode
    end;
cluster_proxy_response_to_internal(#storageattibutes{answer = ErrorStatus} = Attrs) ->
    ErrorCode = fslogic_errors:oneerror_to_posix(fslogic_errors:normalize_error_code(ErrorStatus)),
    {ErrorCode, #st_stat{
        st_atime = Attrs#storageattibutes.atime,
        st_blksize = Attrs#storageattibutes.blksize,
        st_blocks = Attrs#storageattibutes.blocks,
        st_ctime = Attrs#storageattibutes.ctime,
        st_dev = Attrs#storageattibutes.dev,
        st_gid = Attrs#storageattibutes.gid,
        st_ino = Attrs#storageattibutes.ino,
        st_mode = Attrs#storageattibutes.mode,
        st_mtime = Attrs#storageattibutes.mtime,
        st_nlink = Attrs#storageattibutes.nlink,
        st_rdev = Attrs#storageattibutes.rdev,
        st_size = Attrs#storageattibutes.size,
        st_uid = Attrs#storageattibutes.uid
    }};
cluster_proxy_response_to_internal(Response) ->
    ?error("Received unknown ClusterProxy reponse: ~p", [Response]),
    throw({unknown_cluster_proxy_response, Response}).


