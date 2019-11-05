%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests operating on file's cdmi
%%% metadata.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_metadata_req).
-author("Tomasz Lichon").

-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/metadata.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([
    get_transfer_encoding/2, set_transfer_encoding/5,
    get_cdmi_completion_status/2, set_cdmi_completion_status/5,
    get_mimetype/2, set_mimetype/5
]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @equiv get_transfer_encoding_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_transfer_encoding(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
get_transfer_encoding(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?read_attributes]
    ),
    get_transfer_encoding_insecure(UserCtx, FileCtx1).


%%--------------------------------------------------------------------
%% @equiv set_transfer_encoding_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec set_transfer_encoding(user_ctx:ctx(), file_ctx:ctx(),
    xattr:transfer_encoding(), Create :: boolean(), Replace :: boolean()) ->
    fslogic_worker:provider_response().
set_transfer_encoding(UserCtx, FileCtx0, Encoding, Create, Replace) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?write_attributes]
    ),
    set_transfer_encoding_insecure(
        UserCtx, FileCtx1,
        Encoding, Create, Replace
    ).


%%--------------------------------------------------------------------
%% @equiv get_cdmi_completion_status_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_cdmi_completion_status(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
get_cdmi_completion_status(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?read_attributes]
    ),
    get_cdmi_completion_status_insecure(UserCtx, FileCtx1).


%%--------------------------------------------------------------------
%% @equiv set_cdmi_completion_status_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec set_cdmi_completion_status(user_ctx:ctx(), file_ctx:ctx(),
    xattr:cdmi_completion_status(), Create :: boolean(), Replace :: boolean()) ->
    fslogic_worker:provider_response().
set_cdmi_completion_status(UserCtx, FileCtx0, CompletionStatus, Create, Replace) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?write_attributes]
    ),
    set_cdmi_completion_status_insecure(
        UserCtx, FileCtx1,
        CompletionStatus, Create, Replace
    ).


%%--------------------------------------------------------------------
%% @equiv get_mimetype_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_mimetype(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
get_mimetype(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?read_attributes]
    ),
    get_mimetype_insecure(UserCtx, FileCtx1).


%%--------------------------------------------------------------------
%% @equiv set_mimetype_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec set_mimetype(user_ctx:ctx(), file_ctx:ctx(),
    xattr:mimetype(), Create :: boolean(), Replace :: boolean()) ->
    fslogic_worker:provider_response().
set_mimetype(UserCtx, FileCtx0, Mimetype, Create, Replace) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?write_attributes]
    ),
    set_mimetype_insecure(UserCtx, FileCtx1, Mimetype, Create, Replace).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns encoding suitable for rest transfer.
%% @end
%%--------------------------------------------------------------------
-spec get_transfer_encoding_insecure(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
get_transfer_encoding_insecure(_UserCtx, FileCtx) ->
    case xattr:get_by_name(FileCtx, ?TRANSFER_ENCODING_KEY) of
        {ok, Val} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #transfer_encoding{value = Val}};
        {error, not_found} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Sets encoding suitable for rest transfer.
%% @end
%%--------------------------------------------------------------------
-spec set_transfer_encoding_insecure(user_ctx:ctx(), file_ctx:ctx(),
    xattr:transfer_encoding(), Create :: boolean(), Replace :: boolean()) ->
    fslogic_worker:provider_response().
set_transfer_encoding_insecure(_UserCtx, FileCtx, Encoding, Create, Replace) ->
    case xattr:set(FileCtx, ?TRANSFER_ENCODING_KEY, Encoding, Create, Replace) of
        {ok, _} ->
            fslogic_times:update_ctime(FileCtx),
            #provider_response{status = #status{code = ?OK}};
        {error, not_found} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec get_cdmi_completion_status_insecure(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
get_cdmi_completion_status_insecure(_UserCtx, FileCtx) ->
    case xattr:get_by_name(FileCtx, ?CDMI_COMPLETION_STATUS_KEY) of
        {ok, Val} ->
            #provider_response{
                status = #status{code = ?OK},
                provider_response = #cdmi_completion_status{value = Val}
            };
        {error, not_found} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Sets completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec set_cdmi_completion_status_insecure(user_ctx:ctx(), file_ctx:ctx(),
    xattr:cdmi_completion_status(), Create :: boolean(), Replace :: boolean()) ->
    fslogic_worker:provider_response().
set_cdmi_completion_status_insecure(_UserCtx, FileCtx, CompletionStatus, Create, Replace) ->
    case xattr:set(FileCtx, ?CDMI_COMPLETION_STATUS_KEY, CompletionStatus, Create, Replace) of
        {ok, _} ->
            #provider_response{status = #status{code = ?OK}};
        {error, not_found} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns mimetype of file.
%% @end
%%--------------------------------------------------------------------
-spec get_mimetype_insecure(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
get_mimetype_insecure(_UserCtx, FileCtx) ->
    case xattr:get_by_name(FileCtx, ?MIMETYPE_KEY) of
        {ok, Val} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #mimetype{value = Val}};
        {error, not_found} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Sets mimetype of file.
%% @end
%%--------------------------------------------------------------------
-spec set_mimetype_insecure(user_ctx:ctx(), file_ctx:ctx(),
    xattr:mimetype(), Create :: boolean(), Replace :: boolean()) ->
    fslogic_worker:provider_response().
set_mimetype_insecure(_UserCtx, FileCtx, Mimetype, Create, Replace) ->
    case xattr:set(FileCtx, ?MIMETYPE_KEY, Mimetype, Create, Replace) of
        {ok, _} ->
            fslogic_times:update_ctime(FileCtx),
            #provider_response{status = #status{code = ?OK}};
        {error, not_found} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.