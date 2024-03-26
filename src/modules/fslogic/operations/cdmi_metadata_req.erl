%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016-2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests operating on file's
%%% cdmi metadata.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_metadata_req).
-author("Tomasz Lichon").

-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/metadata.hrl").
-include("proto/oneprovider/provider_messages.hrl").

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
%% @doc
%% @equiv get_transfer_encoding_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_transfer_encoding(user_ctx:ctx(), file_ctx:ctx()) ->
    {ok, custom_metadata:transfer_encoding()} | errors:error().
get_transfer_encoding(UserCtx, FileCtx0) ->
    FileCtx1 = file_ctx:assert_file_exists(FileCtx0),
    FileCtx2 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx1,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?read_attributes_mask)]
    ),
    get_transfer_encoding_insecure(UserCtx, FileCtx2).


%%--------------------------------------------------------------------
%% @doc
%% @equiv set_transfer_encoding_insecure/5 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec set_transfer_encoding(
    user_ctx:ctx(),
    file_ctx:ctx(),
    custom_metadata:transfer_encoding(),
    Create :: boolean(),
    Replace :: boolean()
) ->
    ok | errors:error().
set_transfer_encoding(UserCtx, FileCtx0, Encoding, Create, Replace) ->
    FileCtx1 = file_ctx:assert_file_exists(FileCtx0),
    FileCtx2 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx1,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?write_attributes_mask)]
    ),
    set_transfer_encoding_insecure(UserCtx, FileCtx2, Encoding, Create, Replace).


%%--------------------------------------------------------------------
%% @doc
%% @equiv get_cdmi_completion_status_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_cdmi_completion_status(user_ctx:ctx(), file_ctx:ctx()) ->
    {ok, custom_metadata:cdmi_completion_status()} | errors:error().
get_cdmi_completion_status(UserCtx, FileCtx0) ->
    FileCtx1 = file_ctx:assert_file_exists(FileCtx0),
    FileCtx2 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx1,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?read_attributes_mask)]
    ),
    get_cdmi_completion_status_insecure(UserCtx, FileCtx2).


%%--------------------------------------------------------------------
%% @doc
%% @equiv set_cdmi_completion_status_insecure/5 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec set_cdmi_completion_status(
    user_ctx:ctx(),
    file_ctx:ctx(),
    custom_metadata:cdmi_completion_status(),
    Create :: boolean(),
    Replace :: boolean()
) ->
    ok | errors:error().
set_cdmi_completion_status(UserCtx, FileCtx0, CompletionStatus, Create, Replace) ->
    FileCtx1 = file_ctx:assert_file_exists(FileCtx0),
    FileCtx2 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx1,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?write_attributes_mask)]
    ),
    set_cdmi_completion_status_insecure(
        UserCtx, FileCtx2,
        CompletionStatus, Create, Replace
    ).


%%--------------------------------------------------------------------
%% @doc
%% @equiv get_mimetype_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_mimetype(user_ctx:ctx(), file_ctx:ctx()) ->
    {ok, custom_metadata:mimetype()} | errors:error().
get_mimetype(UserCtx, FileCtx0) ->
    FileCtx1 = file_ctx:assert_file_exists(FileCtx0),
    FileCtx2 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx1,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?read_attributes_mask)]
    ),
    get_mimetype_insecure(UserCtx, FileCtx2).


%%--------------------------------------------------------------------
%% @doc
%% @equiv set_mimetype_insecure/5 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec set_mimetype(
    user_ctx:ctx(),
    file_ctx:ctx(),
    custom_metadata:mimetype(),
    Create :: boolean(),
    Replace :: boolean()
) ->
    ok | errors:error().
set_mimetype(UserCtx, FileCtx0, Mimetype, Create, Replace) ->
    FileCtx1 = file_ctx:assert_file_exists(FileCtx0),
    FileCtx2 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx1,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?write_attributes_mask)]
    ),
    set_mimetype_insecure(UserCtx, FileCtx2, Mimetype, Create, Replace).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns encoding suitable for rest transfer.
%% @end
%%--------------------------------------------------------------------
-spec get_transfer_encoding_insecure(user_ctx:ctx(), file_ctx:ctx()) ->
    {ok, custom_metadata:transfer_encoding()} | errors:error().
get_transfer_encoding_insecure(_UserCtx, FileCtx) ->
    case get_cdmi_metadata(FileCtx, ?TRANSFER_ENCODING_KEY) of
        {ok, Val} -> {ok, Val};
        {error, not_found} -> ?ERROR_POSIX(?ENOATTR)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets encoding suitable for rest transfer.
%% @end
%%--------------------------------------------------------------------
-spec set_transfer_encoding_insecure(
    user_ctx:ctx(),
    file_ctx:ctx(),
    custom_metadata:transfer_encoding(),
    Create :: boolean(),
    Replace :: boolean()
) ->
    ok | errors:error().
set_transfer_encoding_insecure(_UserCtx, FileCtx, Encoding, Create, Replace) ->
    case set_cdmi_metadata(FileCtx, ?TRANSFER_ENCODING_KEY, Encoding, Create, Replace) of
        {ok, _} ->
            fslogic_times:update_ctime(FileCtx),
            ok;
        {error, not_found} ->
            ?ERROR_POSIX(?ENOATTR)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec get_cdmi_completion_status_insecure(user_ctx:ctx(), file_ctx:ctx()) ->
    {ok, custom_metadata:cdmi_completion_status()} | errors:error().
get_cdmi_completion_status_insecure(_UserCtx, FileCtx) ->
    case get_cdmi_metadata(FileCtx, ?CDMI_COMPLETION_STATUS_KEY) of
        {ok, Val} -> {ok, Val};
        {error, not_found} -> ?ERROR_POSIX(?ENOATTR)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec set_cdmi_completion_status_insecure(
    user_ctx:ctx(),
    file_ctx:ctx(),
    custom_metadata:cdmi_completion_status(),
    Create :: boolean(),
    Replace :: boolean()
) ->
    ok | errors:error().
set_cdmi_completion_status_insecure(_UserCtx, FileCtx, CompletionStatus, Create, Replace) ->
    case set_cdmi_metadata(FileCtx, ?CDMI_COMPLETION_STATUS_KEY, CompletionStatus, Create, Replace) of
        {ok, _} -> ok;
        {error, not_found} -> ?ERROR_POSIX(?ENOENT)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns mimetype of file.
%% @end
%%--------------------------------------------------------------------
-spec get_mimetype_insecure(user_ctx:ctx(), file_ctx:ctx()) ->
    {ok, custom_metadata:mimetype()} | errors:error().
get_mimetype_insecure(_UserCtx, FileCtx) ->
    case get_cdmi_metadata(FileCtx, ?MIMETYPE_KEY) of
        {ok, Val} -> {ok, Val};
        {error, not_found} -> ?ERROR_POSIX(?ENOATTR)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets mimetype of file.
%% @end
%%--------------------------------------------------------------------
-spec set_mimetype_insecure(
    user_ctx:ctx(),
    file_ctx:ctx(),
    custom_metadata:mimetype(),
    Create :: boolean(),
    Replace :: boolean()
) ->
    ok | errors:error().
set_mimetype_insecure(_UserCtx, FileCtx, Mimetype, Create, Replace) ->
    case set_cdmi_metadata(FileCtx, ?MIMETYPE_KEY, Mimetype, Create, Replace) of
        {ok, _} ->
            fslogic_times:update_ctime(FileCtx),
            ok;
        {error, not_found} ->
            ?ERROR_POSIX(?ENOATTR)
    end.


%% @private
-spec get_cdmi_metadata(file_ctx:ctx(), onedata_file:xattr_name()) ->
    {ok, onedata_file:xattr_value()} | {error, term()}.
get_cdmi_metadata(FileCtx, CdmiAttrName) ->
    custom_metadata:get_xattr(file_ctx:get_logical_uuid_const(FileCtx), CdmiAttrName).


%% @private
-spec set_cdmi_metadata(
    file_ctx:ctx(),
    onedata_file:xattr_name(),
    onedata_file:xattr_value(),
    Create :: boolean(),
    Replace :: boolean()
) ->
    {ok, file_meta:uuid()} | {error, term()}.
set_cdmi_metadata(FileCtx, CdmiAttrName, CdmiAttrValue, Create, Replace) ->
    file_ctx:assert_not_trash_dir_const(FileCtx),
    SyncPolicy = case file_ctx:is_synchronization_enabled(FileCtx) of
        {true, _} -> synchronization_enabled;
        {false, _} -> synchronization_disabled
    end,
    custom_metadata:set_xattr(
        file_ctx:get_logical_uuid_const(FileCtx),
        file_ctx:get_space_id_const(FileCtx),
        CdmiAttrName, CdmiAttrValue, Create, Replace, SyncPolicy
    ).
