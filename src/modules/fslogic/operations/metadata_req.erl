%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Requests operating on file's metadata.
%%% @end
%%%--------------------------------------------------------------------
-module(metadata_req).
-author("Tomasz Lichon").

-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
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
%% Get metadata linked with file
%% @end
%%--------------------------------------------------------------------
-spec get_metadata(fslogic_context:ctx(), file_info:file_info(), custom_metadata:type(),
    custom_metadata:filter(), Inherited :: boolean()) -> fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}, {?read_metadata, 2}]).
get_metadata(_Ctx, File, json, Names, Inherited) ->
    {{uuid, FileUuid} , _File2} = file_info:get_uuid_entry(File),
    case custom_metadata:get_json_metadata(FileUuid, Names, Inherited) of %todo pass file_info
        {ok, Meta} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #metadata{type = json, value = Meta}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end;
get_metadata(_Ctx, File, rdf, _, _) ->
    {{uuid, FileUuid} , _File2} = file_info:get_uuid_entry(File),
    case custom_metadata:get_rdf_metadata(FileUuid) of %todo pass file_info
        {ok, Meta} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #metadata{type = rdf, value = Meta}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Set metadata linked with file
%% @end
%%--------------------------------------------------------------------
-spec set_metadata(fslogic_context:ctx(), file_info:file_info(), custom_metadata:type(),
    custom_metadata:value(), custom_metadata:filter()) -> fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}, {?write_metadata, 2}]).
set_metadata(_Ctx, File, json, Value, Names) ->
    {{uuid, FileUuid} , _File2} = file_info:get_uuid_entry(File),
    {ok, _} = custom_metadata:set_json_metadata(FileUuid, Value, Names), %todo pass file_info
    #provider_response{status = #status{code = ?OK}};
set_metadata(_Ctx, File, rdf, Value, _) ->
    {{uuid, FileUuid} , _File2} = file_info:get_uuid_entry(File),
    {ok, _} = custom_metadata:set_rdf_metadata(FileUuid, Value),
    #provider_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc
%% Remove metadata linked with file
%% @end
%%--------------------------------------------------------------------
-spec remove_metadata(fslogic_context:ctx(), file_info:file_info(), custom_metadata:type()) ->
    fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}, {?write_metadata, 2}]).
remove_metadata(_Ctx, File, json) ->
    {{uuid, FileUuid} , _File2} = file_info:get_uuid_entry(File),
    ok = custom_metadata:remove_json_metadata(FileUuid), %todo pass file_info
    #provider_response{status = #status{code = ?OK}};
remove_metadata(_Ctx, File, rdf) ->
    {{uuid, FileUuid} , _File2} = file_info:get_uuid_entry(File),
    ok = custom_metadata:remove_rdf_metadata(FileUuid), %todo pass file_info
    #provider_response{status = #status{code = ?OK}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================