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

%% API
-export([get_metadata/5, set_metadata/7, remove_metadata/3]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @equiv get_metadata_insecure/5 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_metadata(user_ctx:ctx(), file_ctx:ctx(), custom_metadata:type(),
    custom_metadata:filter(), Inherited :: boolean()) ->
    fslogic_worker:provider_response().
get_metadata(UserCtx, FileCtx0, MetadataType, Names, Inherited) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?read_metadata]
    ),
    get_metadata_insecure(UserCtx, FileCtx1, MetadataType, Names, Inherited).


%%--------------------------------------------------------------------
%% @equiv set_metadata_insecure/5 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec set_metadata(user_ctx:ctx(), file_ctx:ctx(), custom_metadata:type(),
    custom_metadata:value(), custom_metadata:filter(), Create :: boolean(),
    Replace :: boolean()) -> fslogic_worker:provider_response().
set_metadata(UserCtx, FileCtx0, MetadataType, Value, Names, Create, Replace) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?write_metadata]
    ),
    set_metadata_insecure(
        UserCtx, FileCtx1,
        MetadataType, Value, Names, Create, Replace
    ).


%%--------------------------------------------------------------------
%% @equiv remove_metadata_insecure/4 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec remove_metadata(user_ctx:ctx(), file_ctx:ctx(), custom_metadata:type()) ->
    fslogic_worker:provider_response().
remove_metadata(UserCtx, FileCtx0, MetadataType) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?write_metadata]
    ),
    remove_metadata_insecure(UserCtx, FileCtx1, MetadataType).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Gets metadata linked with file.
%% @end
%%--------------------------------------------------------------------
-spec get_metadata_insecure(user_ctx:ctx(), file_ctx:ctx(),
    custom_metadata:type(), custom_metadata:filter(), Inherited :: boolean()) ->
    fslogic_worker:provider_response().
get_metadata_insecure(_UserCtx, FileCtx, json, Names, Inherited) ->
    case json_metadata:get(FileCtx, Names, Inherited) of
        {ok, Meta} ->
            #provider_response{
                status = #status{code = ?OK},
                provider_response = #metadata{type = json, value = Meta}
            };
        {error, not_found} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end;
get_metadata_insecure(_UserCtx, FileCtx, rdf, _, Inherited) ->
    case rdf_metadata:get(FileCtx, Inherited) of
        {ok, Meta} ->
            #provider_response{
                status = #status{code = ?OK},
                provider_response = #metadata{type = rdf, value = Meta}
            };
        {error, not_found} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Sets metadata linked with file.
%% @end
%%--------------------------------------------------------------------
-spec set_metadata_insecure(user_ctx:ctx(), file_ctx:ctx(),
    custom_metadata:type(), custom_metadata:value(), custom_metadata:filter(),
    Create :: boolean(), Replace :: boolean()) ->
    fslogic_worker:provider_response().
set_metadata_insecure(_UserCtx, FileCtx, json, Value, Names, Create, Replace) ->
    {ok, _} = json_metadata:set(FileCtx, Value, Names, Create, Replace),
    #provider_response{status = #status{code = ?OK}};
set_metadata_insecure(_UserCtx, FileCtx, rdf, Value, _, Create, Replace) ->
    {ok, _} = rdf_metadata:set(FileCtx, Value, Create, Replace),
    #provider_response{status = #status{code = ?OK}}.


%%--------------------------------------------------------------------
%% @doc
%% Removes metadata linked with file.
%% @end
%%--------------------------------------------------------------------
-spec remove_metadata_insecure(user_ctx:ctx(), file_ctx:ctx(),
    custom_metadata:type()) -> fslogic_worker:provider_response().
remove_metadata_insecure(_UserCtx, FileCtx, json) ->
    ok = json_metadata:remove(FileCtx),
    #provider_response{status = #status{code = ?OK}};
remove_metadata_insecure(_UserCtx, FileCtx, rdf) ->
    ok = rdf_metadata:remove(FileCtx),
    #provider_response{status = #status{code = ?OK}}.