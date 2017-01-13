%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests operating on file's metadata.
%%% @end
%%%--------------------------------------------------------------------
-module(metadata_req).
-author("Tomasz Lichon").

-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/metadata.hrl").
-include_lib("annotations/include/annotations.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([get_metadata/5, set_metadata/5, remove_metadata/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Gets metadata linked with file
%% @end
%%--------------------------------------------------------------------
-spec get_metadata(user_ctx:ctx(), file_ctx:ctx(), custom_metadata:type(),
    custom_metadata:filter(), Inherited :: boolean()) -> fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}, {?read_metadata, 2}]).
get_metadata(_UserCtx, FileCtx, json, Names, Inherited) ->
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    case custom_metadata:get_json_metadata(FileUuid, Names, Inherited) of %todo pass file_ctx
        {ok, Meta} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #metadata{type = json, value = Meta}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end;
get_metadata(_UserCtx, FileCtx, rdf, _, _) ->
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    case custom_metadata:get_rdf_metadata(FileUuid) of %todo pass file_ctx
        {ok, Meta} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #metadata{type = rdf, value = Meta}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sets metadata linked with file
%% @end
%%--------------------------------------------------------------------
-spec set_metadata(user_ctx:ctx(), file_ctx:ctx(), custom_metadata:type(),
    custom_metadata:value(), custom_metadata:filter()) -> fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}, {?write_metadata, 2}]).
set_metadata(_UserCtx, FileCtx, json, Value, Names) ->
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    {ok, _} = custom_metadata:set_json_metadata(FileUuid, Value, Names), %todo pass file_ctx
    #provider_response{status = #status{code = ?OK}};
set_metadata(_UserCtx, FileCtx, rdf, Value, _) ->
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    {ok, _} = custom_metadata:set_rdf_metadata(FileUuid, Value),
    #provider_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc
%% Removes metadata linked with file
%% @end
%%--------------------------------------------------------------------
-spec remove_metadata(user_ctx:ctx(), file_ctx:ctx(), custom_metadata:type()) ->
    fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}, {?write_metadata, 2}]).
remove_metadata(_UserCtx, FileCtx, json) ->
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    ok = custom_metadata:remove_json_metadata(FileUuid), %todo pass file_ctx
    #provider_response{status = #status{code = ?OK}};
remove_metadata(_UserCtx, FileCtx, rdf) ->
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    ok = custom_metadata:remove_rdf_metadata(FileUuid), %todo pass file_ctx
    #provider_response{status = #status{code = ?OK}}.