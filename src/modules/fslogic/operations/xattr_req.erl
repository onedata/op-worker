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

%% Public API
-export([get_xattr/4, set_xattr/5, remove_xattr/3, list_xattr/4]).
%% Protected API - for use only by other *_req.erl modules
-export([list_xattr_insecure/4]).

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
    case file_ctx:get_active_perms_type(FileCtx, ignore_deleted) of
        {acl, FileCtx2} ->
            case acl_req:get_acl(UserCtx, FileCtx2) of
                #provider_response{
                    status = #status{code = ?OK},
                    provider_response = #acl{value = Acl}
                } ->
                    #fuse_response{
                        status = #status{code = ?OK},
                        fuse_response = #xattr{
                            name = ?ACL_KEY,
                            value = acl:to_json(Acl, cdmi)
                        }
                    }
            end;
        {posix, _} ->
            #fuse_response{status = #status{code = ?ENOATTR}}
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
        #provider_response{} = Other ->
            provider_to_fuse_response(Other)
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
        #provider_response{} = Other ->
            provider_to_fuse_response(Other)
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
        #provider_response{} = Other ->
            provider_to_fuse_response(Other)
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
        #provider_response{} = Other ->
            provider_to_fuse_response(Other)
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
        #provider_response{} = Other ->
            provider_to_fuse_response(Other)
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
set_xattr(UserCtx, FileCtx, #xattr{name = ?ACL_KEY, value = Acl}, _Create, _Replace) ->
    provider_to_fuse_response(
        acl_req:set_acl(UserCtx, FileCtx, acl:from_json(Acl, cdmi))
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
remove_xattr(UserCtx, FileCtx, ?ACL_KEY) ->
    provider_to_fuse_response(acl_req:remove_acl(UserCtx, FileCtx));
remove_xattr(UserCtx, FileCtx, ?JSON_METADATA_KEY) ->
    provider_to_fuse_response(
        metadata_req:remove_metadata(UserCtx, FileCtx, json)
    );
remove_xattr(UserCtx, FileCtx, ?RDF_METADATA_KEY) ->
    provider_to_fuse_response(
        metadata_req:remove_metadata(UserCtx, FileCtx, rdf)
    );
remove_xattr(_UserCtx, _, <<?CDMI_PREFIX_STR, _/binary>>) ->
    throw(?EPERM);
remove_xattr(_UserCtx, _, <<?ONEDATA_PREFIX_STR, _/binary>>) ->
    throw(?EPERM);
remove_xattr(UserCtx, FileCtx, XattrName) ->
    remove_custom_xattr(UserCtx, FileCtx, XattrName).

%%--------------------------------------------------------------------
%% @equiv list_xattr_insecure/4 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec list_xattr(user_ctx:ctx(), file_ctx:ctx(), Inherited :: boolean(),
    ShowInternal :: boolean()) -> fslogic_worker:fuse_response().
list_xattr(UserCtx, FileCtx0, Inherited, ShowInternal) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors]
    ),
    list_xattr_insecure(UserCtx, FileCtx1, Inherited, ShowInternal).

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
                    % acl is kept in file_meta instead of custom_metadata
                    % but is still treated as metadata so ?ACL_KEY
                    % is added to listing if active perms type for
                    % file is acl. Otherwise, to keep backward compatibility,
                    % it is omitted.
                    case file_ctx:get_active_perms_type(FileCtx, ignore_deleted) of
                        {acl, _} ->
                            [?ACL_KEY | XattrList];
                        _ ->
                            XattrList
                    end;
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
get_custom_xattr(UserCtx, FileCtx0, XattrName, Inherited) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?read_metadata]
    ),
    get_custom_xattr_insecure(UserCtx, FileCtx1, XattrName, Inherited).

%%--------------------------------------------------------------------
%% @private
%% @equiv set_custom_xattr_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec set_custom_xattr(user_ctx:ctx(), file_ctx:ctx(), #xattr{},
    Create :: boolean(), Replace :: boolean()) ->
    fslogic_worker:fuse_response().
set_custom_xattr(UserCtx, FileCtx0, Xattr, Create, Replace) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?write_metadata]
    ),
    set_custom_xattr_insecure(UserCtx, FileCtx1, Xattr, Create, Replace).

%%--------------------------------------------------------------------
%% @private
%% @equiv remove_custom_xattr_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec remove_custom_xattr(user_ctx:ctx(), file_ctx:ctx(), xattr:name()) ->
    fslogic_worker:fuse_response().
remove_custom_xattr(UserCtx, FileCtx0, XattrName) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?write_metadata]
    ),
    remove_custom_xattr_insecure(UserCtx, FileCtx1, XattrName).

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
set_custom_xattr_insecure(_UserCtx, FileCtx, #xattr{
    name = XattrName,
    value = XattrValue
}, Create, Replace) ->
    case xattr:set(FileCtx, XattrName, XattrValue, Create, Replace) of
        {ok, _} ->
            fslogic_times:update_ctime(FileCtx),
            #fuse_response{status = #status{code = ?OK}};
        {error, not_found} ->
            #fuse_response{status = #status{code = ?ENOENT}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec remove_custom_xattr_insecure(user_ctx:ctx(), file_ctx:ctx(), xattr:name()) ->
    fslogic_worker:fuse_response().
remove_custom_xattr_insecure(_UserCtx, FileCtx, XattrName) ->
    case xattr:delete_by_name(FileCtx, XattrName) of
        ok ->
            fslogic_times:update_ctime(FileCtx),
            #fuse_response{status = #status{code = ?OK}};
        {error, not_found} ->
            #fuse_response{status = #status{code = ?ENOENT}}
    end.
