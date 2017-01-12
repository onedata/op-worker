%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Requests operating on file extended attributes.
%%% @end
%%%--------------------------------------------------------------------
-module(xattr_req).
-author("Tomasz Lichon").

-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/metadata.hrl").
-include_lib("annotations/include/annotations.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([get_xattr/4, set_xattr/3, remove_xattr/3, list_xattr/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec get_xattr(user_ctx:ctx(), file_ctx:ctx(), xattr:name(), Inherited :: boolean()) ->
    fslogic_worker:provider_response().
get_xattr(UserCtx, FileCtx, ?ACL_KEY, _Inherited) ->
    case acl_req:get_acl(UserCtx, FileCtx) of
        #provider_response{status = #status{code = ?OK}, provider_response = #acl{value = Acl}} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #xattr{name = ?ACL_KEY, value = fslogic_acl:from_acl_to_json_format(Acl)}};
        Other ->
            Other
    end;
get_xattr(UserCtx, FileCtx, ?MIMETYPE_KEY, _Inherited) ->
    case cdmi_metadata_req:get_mimetype(UserCtx, FileCtx) of
        #provider_response{status = #status{code = ?OK}, provider_response = #mimetype{value = Mimetype}} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #xattr{name = ?MIMETYPE_KEY, value = Mimetype}};
        Other ->
            Other
    end;
get_xattr(UserCtx, FileCtx, ?TRANSFER_ENCODING_KEY, _Inherited) ->
    case cdmi_metadata_req:get_transfer_encoding(UserCtx, FileCtx) of
        #provider_response{status = #status{code = ?OK}, provider_response = #transfer_encoding{value = Encoding}} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #xattr{name = ?TRANSFER_ENCODING_KEY, value = Encoding}};
        Other ->
            Other
    end;
get_xattr(UserCtx, FileCtx, ?CDMI_COMPLETION_STATUS_KEY, _Inherited) ->
    case cdmi_metadata_req:get_cdmi_completion_status(UserCtx, FileCtx) of
        #provider_response{status = #status{code = ?OK}, provider_response = #cdmi_completion_status{value = Completion}} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #xattr{name = ?CDMI_COMPLETION_STATUS_KEY, value = Completion}};
        Other ->
            Other
    end;
get_xattr(UserCtx, FileCtx, ?JSON_METADATA_KEY, Inherited) ->
    case metadata_req:get_metadata(UserCtx, FileCtx, json, [], Inherited) of
        #provider_response{status = #status{code = ?OK}, provider_response = #metadata{value = JsonTerm}} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #xattr{name = ?JSON_METADATA_KEY, value = JsonTerm}};
        Other ->
            Other
    end;
get_xattr(UserCtx, FileCtx, ?RDF_METADATA_KEY, Inherited) ->
    case metadata_req:get_metadata(UserCtx, FileCtx, rdf, [], Inherited) of
        #provider_response{status = #status{code = ?OK}, provider_response = #metadata{value = Rdf}} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #xattr{name = ?RDF_METADATA_KEY, value = Rdf}};
        Other ->
            Other
    end;
get_xattr(_UserCtx, _, <<?CDMI_PREFIX_STR, _/binary>>, _) ->
    throw(?EPERM);
get_xattr(_UserCtx, _, <<?ONEDATA_PREFIX_STR, _/binary>>, _) ->
    throw(?EPERM);
get_xattr(UserCtx, FileCtx, XattrName, Inherited) ->
    get_custom_xattr(UserCtx, FileCtx, XattrName, Inherited).

%%--------------------------------------------------------------------
%% @doc
%% Decides if xattr is normal or internal, and routes request to specific function
%% @end
%%--------------------------------------------------------------------
-spec set_xattr(user_ctx:ctx(), file_ctx:ctx(), #xattr{}) ->
    fslogic_worker:provider_response().
set_xattr(UserCtx, FileCtx, #xattr{name = ?ACL_KEY, value = Acl}) ->
    acl_req:set_acl(UserCtx, FileCtx, #acl{value = fslogic_acl:from_json_format_to_acl(Acl)});
set_xattr(UserCtx, FileCtx, #xattr{name = ?MIMETYPE_KEY, value = Mimetype}) ->
    cdmi_metadata_req:set_mimetype(UserCtx, FileCtx, Mimetype);
set_xattr(UserCtx, FileCtx, #xattr{name = ?TRANSFER_ENCODING_KEY, value = Encoding}) ->
    cdmi_metadata_req:set_transfer_encoding(UserCtx, FileCtx, Encoding);
set_xattr(UserCtx, FileCtx, #xattr{name = ?CDMI_COMPLETION_STATUS_KEY, value = Completion}) ->
    cdmi_metadata_req:set_cdmi_completion_status(UserCtx, FileCtx, Completion);
set_xattr(UserCtx, FileCtx, #xattr{name = ?JSON_METADATA_KEY, value = Json}) ->
    metadata_req:set_metadata(UserCtx, FileCtx, json, Json, []);
set_xattr(UserCtx, FileCtx, #xattr{name = ?RDF_METADATA_KEY, value = Rdf}) ->
    metadata_req:set_metadata(UserCtx, FileCtx, rdf, Rdf, []);
set_xattr(_UserCtx, _, #xattr{name = <<?CDMI_PREFIX_STR, _/binary>>}) ->
    throw(?EPERM);
set_xattr(_UserCtx, _, #xattr{name = <<?ONEDATA_PREFIX_STR, _/binary>>}) ->
    throw(?EPERM);
set_xattr(UserCtx, FileCtx, Xattr) ->
    set_custom_xattr(UserCtx, FileCtx, Xattr).

%%--------------------------------------------------------------------
%% @doc
%% Removes file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec remove_xattr(user_ctx:ctx(), file_ctx:ctx(), xattr:name()) ->
    fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}, {?write_metadata, 2}]).
remove_xattr(UserCtx, FileCtx, XattrName) ->
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    case xattr:delete_by_name(FileUuid, XattrName) of %todo pass file_ctx
        ok ->
            fslogic_times:update_ctime({uuid, FileUuid}, user_ctx:get_user_id(UserCtx)), %todo pass file_ctx
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns complete list of extended attributes' keys of a file.
%% @end
%%--------------------------------------------------------------------
-spec list_xattr(user_ctx:ctx(), file_ctx:ctx(), Inherited :: boolean(), ShowInternal :: boolean()) ->
    fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}]).
list_xattr(_UserCtx, FileCtx, Inherited, ShowInternal) ->
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    case xattr:list(FileUuid, Inherited) of
        {ok, XattrList} ->
            FilteredXattrList = case ShowInternal of
                true ->
                    XattrList;
                false ->
                    lists:filter(fun(Key) ->
                        not lists:any(
                            fun(InternalPrefix) ->
                                str_utils:binary_starts_with(Key, InternalPrefix)
                            end, ?METADATA_INTERNAL_PREFIXES)
                    end, XattrList)
            end,
            #provider_response{status = #status{code = ?OK}, provider_response = #xattr_list{names = FilteredXattrList}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec get_custom_xattr(user_ctx:ctx(), file_ctx:ctx(), xattr:name(), Inherited :: boolean()) ->
    fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}, {?read_metadata, 2}]).
get_custom_xattr(_UserCtx, FileCtx, XattrName, Inherited) ->
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    case xattr:get_by_name(FileUuid, XattrName, Inherited) of %todo pass file_ctx
        {ok, XattrValue} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #xattr{name = XattrName, value = XattrValue}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec set_custom_xattr(user_ctx:ctx(), file_ctx:ctx(), #xattr{}) ->
    fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}, {?write_metadata, 2}]).
set_custom_xattr(UserCtx, FileCtx, #xattr{name = XattrName, value = XattrValue}) ->
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    case xattr:save(FileUuid, XattrName, XattrValue) of %todo pass file_ctx
        {ok, _} ->
            fslogic_times:update_ctime({uuid, FileUuid}, user_ctx:get_user_id(UserCtx)), %todo pass file_ctx
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.