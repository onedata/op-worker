%% ===================================================================
%% @author Michał Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: dao_share header
%% @end
%% ===================================================================

-ifndef(DAO_SHARE).
-define(DAO_SHARE, 1).

%% file sharing
-record(share_desc, {file = "", user = non, share_with = all}).

-endif.
