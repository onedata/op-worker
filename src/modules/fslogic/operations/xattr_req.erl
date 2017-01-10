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
-spec get_xattr(user_context:ctx(), file_context:ctx(), xattr:name(), Inherited :: boolean()) ->
    fslogic_worker:provider_response().
get_xattr(Ctx, File, ?ACL_KEY, _Inherited) ->
    case acl_req:get_acl(Ctx, File) of
        #provider_response{status = #status{code = ?OK}, provider_response = #acl{value = Acl}} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #xattr{name = ?ACL_KEY, value = fslogic_acl:from_acl_to_json_format(Acl)}};
        Other ->
            Other
    end;
get_xattr(Ctx, File, ?MIMETYPE_KEY, _Inherited) ->
    case cdmi_metadata_req:get_mimetype(Ctx, File) of
        #provider_response{status = #status{code = ?OK}, provider_response = #mimetype{value = Mimetype}} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #xattr{name = ?MIMETYPE_KEY, value = Mimetype}};
        Other ->
            Other
    end;
get_xattr(Ctx, File, ?TRANSFER_ENCODING_KEY, _Inherited) ->
    case cdmi_metadata_req:get_transfer_encoding(Ctx, File) of
        #provider_response{status = #status{code = ?OK}, provider_response = #transfer_encoding{value = Encoding}} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #xattr{name = ?TRANSFER_ENCODING_KEY, value = Encoding}};
        Other ->
            Other
    end;
get_xattr(Ctx, File, ?CDMI_COMPLETION_STATUS_KEY, _Inherited) ->
    case cdmi_metadata_req:get_cdmi_completion_status(Ctx, File) of
        #provider_response{status = #status{code = ?OK}, provider_response = #cdmi_completion_status{value = Completion}} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #xattr{name = ?CDMI_COMPLETION_STATUS_KEY, value = Completion}};
        Other ->
            Other
    end;
get_xattr(Ctx, File, ?JSON_METADATA_KEY, Inherited) ->
    case metadata_req:get_metadata(Ctx, File, json, [], Inherited) of
        #provider_response{status = #status{code = ?OK}, provider_response = #metadata{value = JsonTerm}} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #xattr{name = ?JSON_METADATA_KEY, value = JsonTerm}};
        Other ->
            Other
    end;
get_xattr(Ctx, File, ?RDF_METADATA_KEY, Inherited) ->
    case metadata_req:get_metadata(Ctx, File, rdf, [], Inherited) of
        #provider_response{status = #status{code = ?OK}, provider_response = #metadata{value = Rdf}} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #xattr{name = ?RDF_METADATA_KEY, value = Rdf}};
        Other ->
            Other
    end;
get_xattr(_Ctx, _, <<?CDMI_PREFIX_STR, _/binary>>, _) ->
    throw(?EPERM);
get_xattr(_Ctx, _, <<?ONEDATA_PREFIX_STR, _/binary>>, _) ->
    throw(?EPERM);
get_xattr(Ctx, File, XattrName, Inherited) ->
    get_custom_xattr(Ctx, File, XattrName, Inherited).

%%--------------------------------------------------------------------
%% @doc
%% Decides if xattr is normal or internal, and routes request to specific function
%% @end
%%--------------------------------------------------------------------
-spec set_xattr(user_context:ctx(), file_context:ctx(), #xattr{}) ->
    fslogic_worker:provider_response().
set_xattr(Ctx, File, #xattr{name = ?ACL_KEY, value = Acl}) ->
    acl_req:set_acl(Ctx, File, #acl{value = fslogic_acl:from_json_format_to_acl(Acl)});
set_xattr(Ctx, File, #xattr{name = ?MIMETYPE_KEY, value = Mimetype}) ->
    cdmi_metadata_req:set_mimetype(Ctx, File, Mimetype);
set_xattr(Ctx, File, #xattr{name = ?TRANSFER_ENCODING_KEY, value = Encoding}) ->
    cdmi_metadata_req:set_transfer_encoding(Ctx, File, Encoding);
set_xattr(Ctx, File, #xattr{name = ?CDMI_COMPLETION_STATUS_KEY, value = Completion}) ->
    cdmi_metadata_req:set_cdmi_completion_status(Ctx, File, Completion);
set_xattr(Ctx, File, #xattr{name = ?JSON_METADATA_KEY, value = Json}) ->
    metadata_req:set_metadata(Ctx, File, json, Json, []);
set_xattr(Ctx, File, #xattr{name = ?RDF_METADATA_KEY, value = Rdf}) ->
    metadata_req:set_metadata(Ctx, File, rdf, Rdf, []);
set_xattr(_Ctx, _, #xattr{name = <<?CDMI_PREFIX_STR, _/binary>>}) ->
    throw(?EPERM);
set_xattr(_Ctx, _, #xattr{name = <<?ONEDATA_PREFIX_STR, _/binary>>}) ->
    throw(?EPERM);
set_xattr(Ctx, File, Xattr) ->
    set_custom_xattr(Ctx, File, Xattr).

%%--------------------------------------------------------------------
%% @doc
%% Removes file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec remove_xattr(user_context:ctx(), file_context:ctx(), xattr:name()) ->
    fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}, {?write_metadata, 2}]).
remove_xattr(Ctx, File, XattrName) ->
    {uuid, FileUuid} = file_context:get_uuid_entry(File),
    case xattr:delete_by_name(FileUuid, XattrName) of %todo pass file_context
        ok ->
            fslogic_times:update_ctime({uuid, FileUuid}, user_context:get_user_id(Ctx)), %todo pass file_context
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns complete list of extended attributes' keys of a file.
%% @end
%%--------------------------------------------------------------------
-spec list_xattr(user_context:ctx(), file_context:ctx(), Inherited :: boolean(), ShowInternal :: boolean()) ->
    fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}]).
list_xattr(_Ctx, File, Inherited, ShowInternal) ->
    {uuid, FileUuid} = file_context:get_uuid_entry(File),
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
%% @doc
%% Returns file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec get_custom_xattr(user_context:ctx(), file_context:ctx(), xattr:name(), Inherited :: boolean()) ->
    fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}, {?read_metadata, 2}]).
get_custom_xattr(_Ctx, File, XattrName, Inherited) ->
    {uuid, FileUuid} = file_context:get_uuid_entry(File),
    case xattr:get_by_name(FileUuid, XattrName, Inherited) of %todo pass file_context
        {ok, XattrValue} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #xattr{name = XattrName, value = XattrValue}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec set_custom_xattr(user_context:ctx(), file_context:ctx(), #xattr{}) ->
    fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}, {?write_metadata, 2}]).
set_custom_xattr(Ctx, File, #xattr{name = XattrName, value = XattrValue}) ->
    {uuid, FileUuid} = file_context:get_uuid_entry(File),
    case xattr:save(FileUuid, XattrName, XattrValue) of %todo pass file_context
        {ok, _} ->
            fslogic_times:update_ctime({uuid, FileUuid}, user_context:get_user_id(Ctx)), %todo pass file_context
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.