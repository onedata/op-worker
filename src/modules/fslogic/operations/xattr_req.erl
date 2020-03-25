%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016-2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests operating on file
%%% extended attributes.
%%% @end
%%%--------------------------------------------------------------------
-module(xattr_req).
-author("Tomasz Lichon").

-include("modules/fslogic/metadata.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").

%% API
-export([list_xattr/4, get_xattr/4, set_xattr/5, remove_xattr/3]).


-define(PROVIDER_OK_RESP(__RESPONSE), #provider_response{
    status = #status{code = ?OK},
    provider_response = __RESPONSE
}).
-define(FUSE_OK_RESP(__RESPONSE), #fuse_response{
    status = #status{code = ?OK},
    fuse_response = __RESPONSE
}).
-define(XATTR(__NAME, __VALUE), #xattr{name = __NAME, value = __VALUE}).


%%%===================================================================
%%% API
%%%===================================================================


-spec list_xattr(
    user_ctx:ctx(),
    file_ctx:ctx(),
    IncludeInherited :: boolean(),
    ShowInternal :: boolean()
) ->
    fslogic_worker:fuse_response().
list_xattr(UserCtx, FileCtx, IncludeInherited, ShowInternal) ->
    {ok, Xattrs} = xattr:list(UserCtx, FileCtx, IncludeInherited, ShowInternal),
    ?FUSE_OK_RESP(#xattr_list{names = Xattrs}).


-spec get_xattr(
    user_ctx:ctx(),
    file_ctx:ctx(),
    custom_metadata:name(),
    Inherited :: boolean()
) ->
    fslogic_worker:fuse_response().
get_xattr(UserCtx, FileCtx, ?ACL_KEY, _Inherited) ->
    case file_ctx:get_active_perms_type(FileCtx, ignore_deleted) of
        {acl, FileCtx2} ->
            case acl_req:get_acl(UserCtx, FileCtx2) of
                ?PROVIDER_OK_RESP(#acl{value = Acl}) ->
                    ?FUSE_OK_RESP(?XATTR(?ACL_KEY, acl:to_json(Acl, cdmi)))
            end;
        {posix, _} ->
            #fuse_response{status = #status{code = ?ENOATTR}}
    end;

get_xattr(UserCtx, FileCtx, ?MIMETYPE_KEY, _Inherited) ->
    case cdmi_metadata_req:get_mimetype(UserCtx, FileCtx) of
        ?PROVIDER_OK_RESP(#mimetype{value = Mimetype}) ->
            ?FUSE_OK_RESP(?XATTR(?MIMETYPE_KEY, Mimetype));
        #provider_response{} = ErrorResponse ->
            provider_to_fuse_response(ErrorResponse)
    end;

get_xattr(UserCtx, FileCtx, ?TRANSFER_ENCODING_KEY, _Inherited) ->
    case cdmi_metadata_req:get_transfer_encoding(UserCtx, FileCtx) of
        ?PROVIDER_OK_RESP(#transfer_encoding{value = Encoding}) ->
            ?FUSE_OK_RESP(?XATTR(?TRANSFER_ENCODING_KEY, Encoding));
        #provider_response{} = ErrorResponse ->
            provider_to_fuse_response(ErrorResponse)
    end;

get_xattr(UserCtx, FileCtx, ?CDMI_COMPLETION_STATUS_KEY, _Inherited) ->
    case cdmi_metadata_req:get_cdmi_completion_status(UserCtx, FileCtx) of
        ?PROVIDER_OK_RESP(#cdmi_completion_status{value = Completion}) ->
            ?FUSE_OK_RESP(?XATTR(?CDMI_COMPLETION_STATUS_KEY, Completion));
        #provider_response{} = ErrorResponse ->
            provider_to_fuse_response(ErrorResponse)
    end;

get_xattr(_UserCtx, _FileCtx, <<?CDMI_PREFIX_STR, _/binary>>, _) ->
    throw(?EPERM);

get_xattr(UserCtx, FileCtx, ?JSON_METADATA_KEY, Inherited) ->
    case metadata_req:get_metadata(UserCtx, FileCtx, json, [], Inherited) of
        ?PROVIDER_OK_RESP(#metadata{value = JsonTerm}) ->
            ?FUSE_OK_RESP(?XATTR(?JSON_METADATA_KEY, JsonTerm));
        #provider_response{} = ErrorResponse ->
            provider_to_fuse_response(ErrorResponse)
    end;

get_xattr(UserCtx, FileCtx, ?RDF_METADATA_KEY, Inherited) ->
    case metadata_req:get_metadata(UserCtx, FileCtx, rdf, [], Inherited) of
        ?PROVIDER_OK_RESP(#metadata{value = Rdf}) ->
            ?FUSE_OK_RESP(?XATTR(?RDF_METADATA_KEY, Rdf));
        #provider_response{} = ErrorResponse ->
            provider_to_fuse_response(ErrorResponse)
    end;

get_xattr(_UserCtx, _FileCtx, <<?ONEDATA_PREFIX_STR, _/binary>>, _) ->
    throw(?EPERM);

get_xattr(UserCtx, FileCtx, XattrName, Inherited) ->
    case xattr:get(UserCtx, FileCtx, XattrName, Inherited) of
        {ok, XattrValue} ->
            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = ?XATTR(XattrName, XattrValue)
            };
        ?ERROR_NOT_FOUND ->
            #fuse_response{status = #status{code = ?ENOATTR}}
    end.


-spec set_xattr(
    user_ctx:ctx(),
    file_ctx:ctx(),
    #xattr{},
    Create :: boolean(),
    Replace :: boolean()
) ->
    fslogic_worker:fuse_response().
set_xattr(UserCtx, FileCtx, ?XATTR(?ACL_KEY, Acl), _Create, _Replace) ->
    provider_to_fuse_response(acl_req:set_acl(
        UserCtx, FileCtx, acl:from_json(Acl, cdmi)
    ));

set_xattr(UserCtx, FileCtx, ?XATTR(?MIMETYPE_KEY, Mimetype), Create, Replace) ->
    provider_to_fuse_response(cdmi_metadata_req:set_mimetype(
        UserCtx, FileCtx, Mimetype, Create, Replace
    ));

set_xattr(UserCtx, FileCtx, ?XATTR(?TRANSFER_ENCODING_KEY, Encoding), Create, Replace) ->
    provider_to_fuse_response(cdmi_metadata_req:set_transfer_encoding(
        UserCtx, FileCtx, Encoding, Create, Replace
    ));

set_xattr(UserCtx, FileCtx, ?XATTR(?CDMI_COMPLETION_STATUS_KEY, Completion), Create, Replace) ->
    provider_to_fuse_response(cdmi_metadata_req:set_cdmi_completion_status(
        UserCtx, FileCtx, Completion, Create, Replace
    ));

set_xattr(_, _, ?XATTR(<<?CDMI_PREFIX_STR, _/binary>>, _), _Create, _Replace) ->
    throw(?EPERM);

set_xattr(UserCtx, FileCtx, ?XATTR(?JSON_METADATA_KEY, Json), Create, Replace) ->
    provider_to_fuse_response(metadata_req:set_metadata(
        UserCtx, FileCtx, json, Json, [], Create, Replace
    ));

set_xattr(UserCtx, FileCtx, ?XATTR(?RDF_METADATA_KEY, Rdf), Create, Replace) ->
    provider_to_fuse_response(metadata_req:set_metadata(
        UserCtx, FileCtx, rdf, Rdf, [], Create, Replace
    ));

set_xattr(_, _, ?XATTR(<<?ONEDATA_PREFIX_STR, _/binary>>, _), _Create, _Replace) ->
    throw(?EPERM);

set_xattr(UserCtx, FileCtx, ?XATTR(XattrName, XattrValue), Create, Replace) ->
    {ok, _} = xattr:set(UserCtx, FileCtx, XattrName, XattrValue, Create, Replace),
    fslogic_times:update_ctime(FileCtx),
    #fuse_response{status = #status{code = ?OK}}.


-spec remove_xattr(user_ctx:ctx(), file_ctx:ctx(), custom_metadata:name()) ->
    fslogic_worker:fuse_response().
remove_xattr(UserCtx, FileCtx, ?ACL_KEY) ->
    provider_to_fuse_response(acl_req:remove_acl(UserCtx, FileCtx));

remove_xattr(_UserCtx, _FileCtx, <<?CDMI_PREFIX_STR, _/binary>>) ->
    throw(?EPERM);

remove_xattr(UserCtx, FileCtx, ?JSON_METADATA_KEY) ->
    provider_to_fuse_response(metadata_req:remove_metadata(UserCtx, FileCtx, json));

remove_xattr(UserCtx, FileCtx, ?RDF_METADATA_KEY) ->
    provider_to_fuse_response(metadata_req:remove_metadata(UserCtx, FileCtx, rdf));

remove_xattr(_UserCtx, _FileCtx, <<?ONEDATA_PREFIX_STR, _/binary>>) ->
    throw(?EPERM);

remove_xattr(UserCtx, FileCtx, XattrName) ->
    ok = xattr:remove(UserCtx, FileCtx, XattrName),
    fslogic_times:update_ctime(FileCtx),
    #fuse_response{status = #status{code = ?OK}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec provider_to_fuse_response(fslogic_worker:provider_response()) ->
    fslogic_worker:fuse_response().
provider_to_fuse_response(#provider_response{status = Status}) ->
    #fuse_response{status = Status}.
