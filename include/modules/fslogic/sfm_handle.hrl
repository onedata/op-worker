%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Storage file manager handler record definition.
%%% @end
%%%--------------------------------------------------------------------
-ifndef(SFM_HANDLER_HRL).
-define(SFM_HANDLER_HRL, 1).

-include("modules/datastore/datastore_specific_models_def.hrl").

%% File handle used by the module
-record(sfm_handle, {
    helper_handle :: helpers:handle(),
    file :: helpers:file(),
    session_id :: session:id(),
    file_uuid :: file_meta:uuid(),
    space_uuid :: file_meta:uuid(),
    storage :: datastore:document() | undefined,
    storage_id :: storage:id(),
    open_mode :: helpers:open_mode(),
    needs_root_privileges :: boolean(),
    is_local = false :: boolean(),
    provider_id :: oneprovider:id()
}).

-endif.