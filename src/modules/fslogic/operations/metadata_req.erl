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

-include("modules/fslogic/metadata.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([get_metadata/5, set_metadata/5, remove_metadata/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Gets metadata linked with file.
%% @end
%%--------------------------------------------------------------------
-spec get_metadata(user_ctx:ctx(), file_ctx:ctx(), custom_metadata:type(),
    custom_metadata:filter(), Inherited :: boolean()) -> fslogic_worker:provider_response().
-check_permissions([traverse_ancestors, ?read_metadata]).
get_metadata(_UserCtx, FileCtx, json, Names, Inherited) ->
    case json_metadata:get(FileCtx, Names, Inherited) of
        {ok, Meta} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #metadata{type = json, value = Meta}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end;
get_metadata(_UserCtx, FileCtx, rdf, _, _) ->
    case rdf_metadata:get(FileCtx) of
        {ok, Meta} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #metadata{type = rdf, value = Meta}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sets metadata linked with file.
%% @end
%%--------------------------------------------------------------------
-spec set_metadata(user_ctx:ctx(), file_ctx:ctx(), custom_metadata:type(),
    custom_metadata:value(), custom_metadata:filter()) -> fslogic_worker:provider_response().
-check_permissions([traverse_ancestors, ?write_metadata]).
set_metadata(_UserCtx, FileCtx, json, Value, Names) ->
    {ok, _} = json_metadata:set(FileCtx, Value, Names),
    #provider_response{status = #status{code = ?OK}};
set_metadata(_UserCtx, FileCtx, rdf, Value, _) ->
    {ok, _} = rdf_metadata:set(FileCtx, Value),
    #provider_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc
%% Removes metadata linked with file.
%% @end
%%--------------------------------------------------------------------
-spec remove_metadata(user_ctx:ctx(), file_ctx:ctx(), custom_metadata:type()) ->
    fslogic_worker:provider_response().
-check_permissions([traverse_ancestors, ?write_metadata]).
remove_metadata(_UserCtx, FileCtx, json) ->
    ok = json_metadata:remove(FileCtx),
    #provider_response{status = #status{code = ?OK}};
remove_metadata(_UserCtx, FileCtx, rdf) ->
    ok = rdf_metadata:remove(FileCtx),
    #provider_response{status = #status{code = ?OK}}.