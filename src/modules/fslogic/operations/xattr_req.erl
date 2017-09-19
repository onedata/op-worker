%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests operating on file extended
%%% attributes.
%%% @end
%%%--------------------------------------------------------------------
-module(xattr_req).
-author("Tomasz Lichon").

-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/metadata.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([get_xattr/4, set_xattr/5, remove_xattr/3, list_xattr/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec get_xattr(user_ctx:ctx(), file_ctx:ctx(), xattr:name(), Inherited :: boolean()) ->
    fslogic_worker:fuse_response().
get_xattr(UserCtx, FileCtx, ?ACL_KEY, _Inherited) ->
    case acl_req:get_acl(UserCtx, FileCtx) of
        #provider_response{
            status = #status{code = ?OK},
            provider_response = #acl{value = Acl}
        } ->
            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = #xattr{
                    name = ?ACL_KEY,
                    value = acl_logic:from_acl_to_json_format(Acl)
                }
            };
        Other ->
            Other
    end;
get_xattr(UserCtx, FileCtx, ?MIMETYPE_KEY, _Inherited) ->
    case cdmi_metadata_req:get_mimetype(UserCtx, FileCtx) of
        #provider_response{
            status = #status{code = ?OK},
            provider_response = #mimetype{value = Mimetype}
        } ->
            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = #xattr{
                    name = ?MIMETYPE_KEY,
                    value = Mimetype
                }
            };
        Other ->
            Other
    end;
get_xattr(UserCtx, FileCtx, ?TRANSFER_ENCODING_KEY, _Inherited) ->
    case cdmi_metadata_req:get_transfer_encoding(UserCtx, FileCtx) of
        #provider_response{
            status = #status{code = ?OK},
            provider_response = #transfer_encoding{value = Encoding}
        } ->
            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = #xattr{
                    name = ?TRANSFER_ENCODING_KEY,
                    value = Encoding
                }
            };
        Other ->
            Other
    end;
get_xattr(UserCtx, FileCtx, ?CDMI_COMPLETION_STATUS_KEY, _Inherited) ->
    case cdmi_metadata_req:get_cdmi_completion_status(UserCtx, FileCtx) of
        #provider_response{
            status = #status{code = ?OK},
            provider_response = #cdmi_completion_status{value = Completion}
        } ->
            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = #xattr{
                    name = ?CDMI_COMPLETION_STATUS_KEY,
                    value = Completion}
            };
        Other ->
            Other
    end;
get_xattr(UserCtx, FileCtx, ?JSON_METADATA_KEY, Inherited) ->
    case metadata_req:get_metadata(UserCtx, FileCtx, json, [], Inherited) of
        #provider_response{
            status = #status{code = ?OK},
            provider_response = #metadata{value = JsonTerm}
        } ->
            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = #xattr{
                    name = ?JSON_METADATA_KEY,
                    value = JsonTerm}
            };
        Other ->
            Other
    end;
get_xattr(UserCtx, FileCtx, ?RDF_METADATA_KEY, Inherited) ->
    case metadata_req:get_metadata(UserCtx, FileCtx, rdf, [], Inherited) of
        #provider_response{
            status = #status{code = ?OK},
            provider_response = #metadata{value = Rdf}
        } ->
            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = #xattr{
                    name = ?RDF_METADATA_KEY,
                    value = Rdf}
            };
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
-spec set_xattr(user_ctx:ctx(), file_ctx:ctx(), #xattr{},
    Create :: boolean(), Replace :: boolean()) ->
    fslogic_worker:fuse_response().
set_xattr(UserCtx, FileCtx, #xattr{name = ?ACL_KEY, value = Acl}, Create, Replace) ->
    provider_to_fuse_response(
        acl_req:set_acl(UserCtx, FileCtx, #acl{value = acl_logic:from_json_format_to_acl(Acl)}, Create, Replace)
    );
set_xattr(UserCtx, FileCtx, #xattr{name = ?MIMETYPE_KEY, value = Mimetype}, Create, Replace) ->
    provider_to_fuse_response(
        cdmi_metadata_req:set_mimetype(UserCtx, FileCtx, Mimetype, Create, Replace)
    );
set_xattr(UserCtx, FileCtx, #xattr{name = ?TRANSFER_ENCODING_KEY, value = Encoding}, Create, Replace) ->
    provider_to_fuse_response(
        cdmi_metadata_req:set_transfer_encoding(UserCtx, FileCtx, Encoding, Create, Replace)
    );
set_xattr(UserCtx, FileCtx, #xattr{name = ?CDMI_COMPLETION_STATUS_KEY, value = Completion}, Create, Replace) ->
    provider_to_fuse_response(
        cdmi_metadata_req:set_cdmi_completion_status(UserCtx, FileCtx, Completion, Create, Replace)
    );
set_xattr(UserCtx, FileCtx, #xattr{name = ?JSON_METADATA_KEY, value = Json}, Create, Replace) ->
    provider_to_fuse_response(
        metadata_req:set_metadata(UserCtx, FileCtx, json, Json, [], Create, Replace)
    );
set_xattr(UserCtx, FileCtx, #xattr{name = ?RDF_METADATA_KEY, value = Rdf}, Create, Replace) ->
    provider_to_fuse_response(
        metadata_req:set_metadata(UserCtx, FileCtx, rdf, Rdf, [], Create, Replace)
    );
set_xattr(_UserCtx, _, #xattr{name = <<?CDMI_PREFIX_STR, _/binary>>}, _Create, _Replace) ->
    throw(?EPERM);
set_xattr(_UserCtx, _, #xattr{name = <<?ONEDATA_PREFIX_STR, _/binary>>}, _Create, _Replace) ->
    throw(?EPERM);
set_xattr(UserCtx, FileCtx, Xattr, Create, Replace) ->
    set_custom_xattr(UserCtx, FileCtx, Xattr, Create, Replace).

%%--------------------------------------------------------------------
%% @equiv remove_xattr_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec remove_xattr(user_ctx:ctx(), file_ctx:ctx(), xattr:name()) ->
    fslogic_worker:fuse_response().
remove_xattr(_UserCtx, FileCtx, XattrName) ->
    check_permissions:execute(
        [traverse_ancestors, ?write_metadata],
        [_UserCtx, FileCtx, XattrName],
        fun remove_xattr_insecure/3).

%%--------------------------------------------------------------------
%% @equiv list_xattr_insecure/4 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec list_xattr(user_ctx:ctx(), file_ctx:ctx(), Inherited :: boolean(),
    ShowInternal :: boolean()) -> fslogic_worker:fuse_response().
list_xattr(_UserCtx, FileCtx, Inherited, ShowInternal) ->
    check_permissions:execute(
        [traverse_ancestors],
        [_UserCtx, FileCtx, Inherited, ShowInternal],
        fun list_xattr_insecure/4).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Convert provider_reponse to fuse_response
%% @end
%%--------------------------------------------------------------------
-spec provider_to_fuse_response(fslogic_worker:provider_response()) ->
    fslogic_worker:fuse_response().
provider_to_fuse_response(#provider_response{status = Status}) ->
    #fuse_response{status = Status}.

%%--------------------------------------------------------------------
%% @doc
%% Removes file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec remove_xattr_insecure(user_ctx:ctx(), file_ctx:ctx(), xattr:name()) ->
    fslogic_worker:fuse_response().
remove_xattr_insecure(_UserCtx, FileCtx, XattrName) ->
    case xattr:delete_by_name(FileCtx, XattrName) of
        ok ->
            fslogic_times:update_ctime(FileCtx),
            #fuse_response{status = #status{code = ?OK}};
        {error, not_found} ->
            #fuse_response{status = #status{code = ?ENOENT}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns complete list of extended attributes' keys of a file.
%% @end
%%--------------------------------------------------------------------
-spec list_xattr_insecure(user_ctx:ctx(), file_ctx:ctx(), Inherited :: boolean(),
    ShowInternal :: boolean()) -> fslogic_worker:fuse_response().
list_xattr_insecure(_UserCtx, FileCtx, Inherited, ShowInternal) ->
    case file_ctx:file_exists_const(FileCtx) of
        true ->
            {ok, XattrList} = xattr:list(FileCtx, Inherited),
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
            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = #xattr_list{names = FilteredXattrList}
            };
        false ->
            #fuse_response{status = #status{code = ?ENOENT}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @equiv get_custom_xattr_insecure/4 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_custom_xattr(user_ctx:ctx(), file_ctx:ctx(), xattr:name(),
    Inherited :: boolean()) -> fslogic_worker:fuse_response().
get_custom_xattr(_UserCtx, FileCtx, XattrName, Inherited) ->
    check_permissions:execute(
        [traverse_ancestors, ?read_metadata],
        [_UserCtx, FileCtx, XattrName, Inherited],
        fun get_custom_xattr_insecure/4).

%%--------------------------------------------------------------------
%% @private
%% @equiv set_custom_xattr_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec set_custom_xattr(user_ctx:ctx(), file_ctx:ctx(), #xattr{},
    Create :: boolean(), Replace :: boolean()) ->
    fslogic_worker:fuse_response().
set_custom_xattr(_UserCtx, FileCtx, Xattr, Create, Replace) ->
    check_permissions:execute(
        [traverse_ancestors, ?write_metadata],
        [_UserCtx, FileCtx, Xattr, Create, Replace],
        fun set_custom_xattr_insecure/5).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec get_custom_xattr_insecure(user_ctx:ctx(), file_ctx:ctx(), xattr:name(),
    Inherited :: boolean()) -> fslogic_worker:fuse_response().
get_custom_xattr_insecure(_UserCtx, FileCtx, XattrName, Inherited) ->
    case xattr:get_by_name(FileCtx, XattrName, Inherited) of
        {ok, XattrValue} ->
            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = #xattr{name = XattrName, value = XattrValue}
            };
        {error, not_found} ->
            #fuse_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec set_custom_xattr_insecure(user_ctx:ctx(), file_ctx:ctx(), #xattr{},
    Create :: boolean(), Replace :: boolean()) ->
    fslogic_worker:fuse_response().
set_custom_xattr_insecure(_UserCtx, FileCtx,
    #xattr{name = XattrName, value = XattrValue}, Create, Replace
) ->
    case xattr:set(FileCtx, XattrName, XattrValue, Create, Replace) of
        {ok, _} ->
            fslogic_times:update_ctime(FileCtx),
            #fuse_response{status = #status{code = ?OK}};
        {error, not_found} ->
            #fuse_response{status = #status{code = ?ENOENT}}
    end.