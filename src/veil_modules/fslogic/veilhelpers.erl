%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module shoul be used as an proxy to veilhelpers_nif module.
%%       This module controls way of accessing veilhelpers_nif methods.
%% @end
%% ===================================================================

-module(veilhelpers).

-include("veil_modules/dao/dao_vfs.hrl").
-include_lib("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").
-include("remote_file_management_pb.hrl").
-include("communication_protocol_pb.hrl").

-export([exec/2, exec/3, exec/4, exec/5]).
%% ===================================================================
%% API
%% ===================================================================


%% exec/3
%% ====================================================================
%% @doc Executes apply(veilhelper_nif, Method, Args) through slave node. <br/>
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
%% @doc Executes apply(veilhelper_nif, Method, Args) through slave node. <br/>
%% @end
-spec exec(Method :: atom(), [Arg :: term()]) ->
    {error, Reason :: term()} | Response when Response :: term().
%% ====================================================================
exec(Method, Args) when is_atom(Method), is_list(Args) ->
    [EGroup | _] = fslogic_context:get_fs_group_ctx(),
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
    ?debug("VeilHelpers Storage CTX ~p ~p", [UserName, GroupId]),
    ?debug("veilhelpers:exec with args: ~p ~p", [Method, Args1]),

    case Args of
        ["ClusterProxy", HelperArgs | MethodArgs] ->
            reroute_to_remote_provider(HelperArgs, Method, MethodArgs);
        _ ->
            case gsi_handler:call(veilhelpers_nif, Method, Args1) of
                {error, 'NIF_not_loaded'} ->
                    ok = load_veilhelpers(),
                    gsi_handler:call(veilhelpers_nif, Method, Args1);
                Other -> Other
            end
    end.


%% load_veilhelpers/0
%% ====================================================================
%% @doc Loads NIF library into slave node. Nodes are started and managed by {@link gsi_handler}
%% @end
-spec load_veilhelpers() -> ok | {error, Reason :: term()}.
%% ====================================================================
load_veilhelpers() ->
    {ok, Prefix} = application:get_env(?APP_Name, nif_prefix),
    case gsi_handler:call(veilhelpers_nif, start, [atom_to_list(Prefix)]) of 
        ok -> ok;
        {error,{reload, _}} -> ok;
        {error, Reason} -> 
            ?error("Could not load veilhelpers NIF lib due to error: ~p", [Reason]),
            {error, Reason}
    end.

reroute_to_remote_provider(_, open, _) ->
    0;
reroute_to_remote_provider(_, release, _) ->
    0;
reroute_to_remote_provider([SpaceId] = _HelperArgs, mknod, [FileId, Mode, _Dev]) ->
    RequestBody = #createfile{file_id = FileId, mode = Mode},
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

do_reroute(SpaceId, RequestBody) ->
    {ok, #space_info{providers = Providers}} = fslogic_objects:get_space({uuid, SpaceId}),

    [RerouteToProvider | _] = Providers,
    {ok, #{<<"urls">> := URLs}} = registry_providers:get_provider_info(RerouteToProvider),
    ?info("VeilHelper Reroute to: ~p", [URLs]),
    try
        Response = provider_proxy:reroute_pull_message(RerouteToProvider, fslogic_context:get_access_token(),
            fslogic_context:get_fuse_id(), #remotefilemangement{space_id = SpaceId, input = RequestBody, message_type = atom_to_list(element(1, RequestBody))}),
        cluster_proxy_response_to_internel(Response)
    catch
        Type:Reason ->
            ?error_stacktrace("Unable to process remote files manager request to provider ~p due to: ~p", [RerouteToProvider, {Type, Reason}]),
            throw({unable_to_reroute_message, Reason})
    end.

cluster_proxy_response_to_internel(#atom{value = ErrorStatus}) ->
    ErrorCode = fslogic_errors:veilerror_to_posix(fslogic_errors:normalize_error_code(ErrorStatus)),
    -ErrorCode;
cluster_proxy_response_to_internel(#filedata{answer_status = ErrorStatus, data = Data}) ->
    ErrorCode = fslogic_errors:veilerror_to_posix(fslogic_errors:normalize_error_code(ErrorStatus)),
    case ErrorCode of
        0 -> {size(Data), Data};
        _ -> {-ErrorCode, Data}
    end;
cluster_proxy_response_to_internel(#writeinfo{answer_status = ErrorStatus, bytes_written = BytesWritten}) ->
    ErrorCode = fslogic_errors:veilerror_to_posix(fslogic_errors:normalize_error_code(ErrorStatus)),
    case ErrorCode of
        0 -> BytesWritten;
        _ -> -ErrorCode
    end.

