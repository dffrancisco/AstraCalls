package main

import (
	"net/http/httptest"
	"testing"
)

// Dois Chatwoots DIFERENTES (kp e fuscao) com a MESMA conta 1 no mesmo AstraCalls,
// mais uma sessão da conta 6 num terceiro Chatwoot.
func brokerDuasInstancias() *Broker {
	b := NewBroker()
	b.AccountForSession = func(sid string) int {
		switch sid {
		case "sess-kp", "sess-fuscao":
			return 1
		case "sess-conta6":
			return 6
		}
		return 0
	}
	b.HostForSession = func(sid string) string {
		switch sid {
		case "sess-kp":
			return "kp.neyap.com.br"
		case "sess-fuscao":
			return "fuscao.neyap.com.br"
		case "sess-conta6":
			return "chat.neyap.com.br"
		}
		return ""
	}
	return b
}

// call-status e call-ended saíam por broadcast GLOBAL: chegavam até em outra conta.
func TestCallStatusEEndedEscopadosPorSessao(t *testing.T) {
	b := brokerDuasInstancias()
	kp := b.subscribeScoped("astrachat_acc1", 1, "kp.neyap.com.br")
	conta6 := b.subscribeScoped("astrachat_acc6", 6, "chat.neyap.com.br")

	b.upsertCall(CallRecord{SessionID: "sess-fuscao", CallID: "c1", Direction: "inbound", Status: StatusRinging})
	b.endCall("c1", "user_ended")

	for nome, s := range map[string]*subscriber{"kp": kp, "conta 6": conta6} {
		evs := drain(s)
		if hasType(evs, "call-status") {
			t.Errorf("%s NÃO deveria receber call-status de sessão da fuscao", nome)
		}
		if hasType(evs, "call-ended") {
			t.Errorf("%s NÃO deveria receber call-ended de sessão da fuscao", nome)
		}
	}
}

// O retrato call-list é a lista de TODAS as chamadas vivas; cada assinante só pode
// ver as das sessões do seu escopo (o listener do Chatwoot toca a partir dele).
func TestCallListFiltradoPorAssinante(t *testing.T) {
	b := brokerDuasInstancias()
	admin := b.subscribe("painel", 0)
	kp := b.subscribeScoped("astrachat_acc1", 1, "kp.neyap.com.br")

	b.upsertCall(CallRecord{SessionID: "sess-kp", CallID: "da-kp", Direction: "inbound", Status: StatusRinging})
	b.upsertCall(CallRecord{SessionID: "sess-fuscao", CallID: "da-fuscao", Direction: "inbound", Status: StatusRinging})
	b.upsertCall(CallRecord{SessionID: "sess-conta6", CallID: "da-conta6", Direction: "inbound", Status: StatusRinging})

	ids := func(evs []map[string]any) map[string]bool {
		out := map[string]bool{}
		var ultimo map[string]any
		for _, e := range evs {
			if e["type"] == "call-list" {
				ultimo = e
			}
		}
		if ultimo == nil {
			return out
		}
		for _, c := range ultimo["calls"].([]any) {
			out[c.(map[string]any)["callId"].(string)] = true
		}
		return out
	}

	got := ids(drain(kp))
	if !got["da-kp"] || got["da-fuscao"] || got["da-conta6"] || len(got) != 1 {
		t.Errorf("call-list da kp deveria ter só a chamada da kp, veio %v", got)
	}
	if len(ids(drain(admin))) != 3 {
		t.Error("painel admin deveria ver as 3 chamadas no call-list")
	}
}

// O listener do Chatwoot (lado servidor) não manda Origin: ele se identifica pelo
// chatwootUrl. Só vale host de Chatwoot conhecido — alias/PWA ficam sem host.
func TestHostDoAssinanteSSE(t *testing.T) {
	scopeHost := func(raw string) string {
		if h := chatwootHost(raw); h == "kp.neyap.com.br" || h == "fuscao.neyap.com.br" {
			return h
		}
		return ""
	}
	casos := []struct {
		nome, url, origin, want string
	}{
		{"listener com chatwootUrl", "/api/events?accountId=1&clientId=astrachat_acc1&chatwootUrl=https%3A%2F%2Ffuscao.neyap.com.br", "", "fuscao.neyap.com.br"},
		{"chatwootUrl vence origin", "/api/events?accountId=1&chatwootUrl=https%3A%2F%2Ffuscao.neyap.com.br", "https://kp.neyap.com.br", "fuscao.neyap.com.br"},
		{"navegador pelo origin", "/api/events?accountId=1", "https://kp.neyap.com.br", "kp.neyap.com.br"},
		{"chatwootUrl desconhecido cai no origin", "/api/events?accountId=1&chatwootUrl=https%3A%2F%2Falias.neyap.com.br", "https://kp.neyap.com.br", "kp.neyap.com.br"},
		{"origin desconhecido (alias/PWA)", "/api/events?accountId=1", "https://appkp.neyap.com.br", ""},
		{"listener antigo sem nada", "/api/events?accountId=1&clientId=astrachat_acc1", "", ""},
	}
	for _, c := range casos {
		r := httptest.NewRequest("GET", c.url, nil)
		if c.origin != "" {
			r.Header.Set("Origin", c.origin)
		}
		if got := sseHost(r, scopeHost); got != c.want {
			t.Errorf("%s: host = %q, quero %q", c.nome, got, c.want)
		}
	}
}

// Ponta a ponta com a ligação real do server.go: o navegador da kp entra pelo Origin
// e o listener da fuscao pelo chatwootUrl — a chamada da fuscao só chega na fuscao.
func TestSessionManagerSeparaChatwootsDaMesmaConta(t *testing.T) {
	broker := NewBroker()
	mgr := &SessionManager{broker: broker, sessions: map[string]*Session{}}
	broker.AccountForSession = mgr.accountIDForSession
	broker.HostForSession = mgr.hostForSession
	for id, url := range map[string]string{"sess-kp": "https://kp.neyap.com.br", "sess-fuscao": "https://fuscao.neyap.com.br/"} {
		s := &Session{id: id, mgr: mgr, reg: newCallRegistry()}
		s.setChatwoot(ChatwootConfig{URL: url, AccountID: 1, AccountToken: "t", InboxID: 1})
		mgr.register(s)
	}

	rKp := httptest.NewRequest("GET", "/api/events?accountId=1", nil)
	rKp.Header.Set("Origin", "https://kp.neyap.com.br")
	rKpListener := httptest.NewRequest("GET", "/api/events?accountId=1&clientId=astrachat_acc1&chatwootUrl=https%3A%2F%2Fkp.neyap.com.br", nil)
	rFuscao := httptest.NewRequest("GET", "/api/events?accountId=1&clientId=astrachat_acc1&chatwootUrl=https%3A%2F%2Ffuscao.neyap.com.br", nil)
	kp := broker.subscribeScoped("navegador-kp", 1, sseHost(rKp, mgr.chatwootScopeHost))
	kpListener := broker.subscribeScoped("astrachat_acc1", 1, sseHost(rKpListener, mgr.chatwootScopeHost))
	fuscao := broker.subscribeScoped("astrachat_acc1", 1, sseHost(rFuscao, mgr.chatwootScopeHost))

	broker.emitIncoming("sess-fuscao", "call-1", "5561999999999", "5561999999999", "", false)

	if hasType(drain(kp), "incoming") {
		t.Error("navegador da kp NÃO deveria receber a chamada da fuscao")
	}
	if hasType(drain(kpListener), "incoming") {
		t.Error("listener da kp NÃO deveria receber a chamada da fuscao (o bug de 26/10)")
	}
	if !hasType(drain(fuscao), "incoming") {
		t.Error("listener da fuscao deveria receber a própria chamada")
	}
}
