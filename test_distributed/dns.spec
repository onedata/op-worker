%% ===================================================================
%% @author Bartosz Polnik
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file is a specification of a test. The test can be found
%% in dns directory. The test description can be found in dns_SUITE.erl file.
%% @end
%% ===================================================================

%% slave nodes
{node, ccm, 'dns_ccm@localhost'}.

%% start nodes
{init, [ccm], [{node_start, [{monitor_master, true}]}]}.

%% log directories (all_nodes does not include master)
%% {logdir, master, "."}.
{logdir, all_nodes, "./dns"}.

%% test castes to be run
{alias, dns, "./dns"}.

{cases, [ccm], dns, dns_SUITE, all}.

