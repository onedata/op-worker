%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements data_backend_behaviour and is used to synchronize
%%% the file-acl model used in Ember application.
%%% @end
%%%-------------------------------------------------------------------
-module(file_acl_data_backend).
-behavior(data_backend_behaviour).
-author("Lukasz Opiola").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% data_backend_behaviour callbacks
-export([init/0, terminate/0]).
-export([find/2, find_all/1, find_query/2]).
-export([create_record/2, update_record/3, delete_record/2]).

%%%===================================================================
%%% data_backend_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback init/0.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback terminate/0.
%% @end
%%--------------------------------------------------------------------
-spec terminate() -> ok.
terminate() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find/2.
%% @end
%%--------------------------------------------------------------------
-spec find(ResourceType :: binary(), Id :: binary()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find(<<"file-acl">>, FileId) ->
    SessionId = g_session:get_session_id(),
    file_acl_record(SessionId, FileId).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
find_all(<<"file-acl">>) ->
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_query/2.
%% @end
%%--------------------------------------------------------------------
-spec find_query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_query(<<"file-acl">>, _Data) ->
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
create_record(<<"file-acl">>, Data) ->
    Id = proplists:get_value(<<"file">>, Data),
    case update_record(<<"file-acl">>, Id, Data) of
        ok ->
            file_acl_record(?ROOT_SESS_ID, Id);
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | gui_error:error_result().
update_record(<<"file-acl">>, FileId, Data) ->
    try
        SessionId = g_session:get_session_id(),
        Acl = acl_utils:json_to_acl(Data),
        case logical_file_manager:set_acl(SessionId, {guid, FileId}, Acl) of
            ok ->
                ok;
            {error, ?EACCES} ->
                gui_error:report_warning(
                    <<"Cannot change ACL - access denied.">>
                )
        end
    catch _:_ ->
        gui_error:report_warning(<<"Cannot change ACL.">>)
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | gui_error:error_result().
delete_record(<<"file-acl">>, FileId) ->
    SessionId = g_session:get_session_id(),
    case logical_file_manager:remove_acl(SessionId, {guid, FileId}) of
        ok ->
            ok;
        {error, ?EACCES} ->
            gui_error:report_warning(
                <<"Cannot remove ACL - access denied.">>)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Constructs a file acl record for given FileId.
%% @end
%%--------------------------------------------------------------------
-spec file_acl_record(SessionId :: session:id(), fslogic_worker:file_guid()) ->
    {ok, proplists:proplist()}.
file_acl_record(SessionId, FileId) ->
    case logical_file_manager:get_acl(SessionId, {guid, FileId}) of
        {error, ?ENOENT} ->
            gui_error:report_error(<<"No such file or directory.">>);
        {error, ?ENOATTR} ->
            gui_error:report_error(<<"No ACL defined.">>);
        {error, ?EACCES} ->
            gui_error:report_error(<<"Cannot read ACL - access denied.">>);
        {ok, Acl} ->
            Res = acl_utils:acl_to_json(FileId, Acl),
            {ok, Res}
    end.