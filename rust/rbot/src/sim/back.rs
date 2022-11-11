use pyo3::{pyclass, pymethods, Py, PyAny, Python};
use rusqlite::params;

use crate::{
    common::{
        order::{OrderResult, OrderSide, Trade},
        time::{MicroSec, CEIL, SEC},
    },
    db::open_db,
    exchange::ftx::FtxMarket,
    sim::session::DummySession,
};

#[pyclass]
pub struct BackTester {
    exchange_name: String,
    market_name: String,
    agent_on_tick: bool,
    agent_on_clock: bool,
    agent_on_update: bool,
}

#[pymethods]
impl BackTester {
    #[new]
    pub fn new(exchange_name: &str, market_name: &str) -> Self {
        return BackTester {
            exchange_name: exchange_name.to_string(),
            market_name: market_name.to_string(),
            agent_on_tick: false,
            agent_on_clock: false,
            agent_on_update: false,
        };
    }

    pub fn run(&mut self, agent: &PyAny) -> Vec<OrderResult> {
        self.agent_on_tick = self.has_want_event(agent, "on_tick");
        self.agent_on_clock = self.has_want_event(agent, "on_clock");
        self.agent_on_update = self.has_want_event(agent, "on_update");

        log::debug!("want on tick  {:?}", self.agent_on_tick);
        log::debug!("want on clock {:?}", self.agent_on_clock);
        log::debug!("want on event {:?}", self.agent_on_update);

        let clock_interval = self.clock_interval(agent);
        log::debug!("clock interval {:?}", clock_interval);

        let db = open_db(self.exchange_name.as_str(), self.market_name.as_str());
        let mut statement = db.select_all_statement();

        let mut order_history: Vec<OrderResult> = vec![];

        Python::with_gil(|py| {
            let iter = statement
                .query_map(params![], |row| {
                    let bs_str: String = row.get_unwrap(1);
                    let bs = OrderSide::from_str(bs_str.as_str());

                    Ok(Trade {
                        time: row.get_unwrap(0),
                        price: row.get_unwrap(2),
                        size: row.get_unwrap(3),
                        order_side: bs,
                        liquid: row.get_unwrap(4),
                        id: row.get_unwrap(5),
                    })
                })
                .unwrap();

            let mut session =
                DummySession::new(self.exchange_name.as_str(), self.market_name.as_str());
            let mut s = Py::new(py, session).unwrap();
            let mut last_clock: i64 = 0;

            for trade in iter {
                match trade {
                    Ok(t) => {
                        if self.agent_on_clock {
                            let current_clock = CEIL(t.time, clock_interval);
                            if current_clock != last_clock {
                                s = self.clock(s, agent, current_clock);
                                last_clock = current_clock;
                            }
                        }

                        session = s.extract::<DummySession>(py).unwrap();

                        //let results = session.main_exec_event(t.time, t.order_side, t.price, t.size);

                        let mut tick_result: Vec<OrderResult> = vec![];

                        session.main_exec_event(
                            &mut tick_result,
                            t.time,
                            t.order_side,
                            t.price,
                            t.size,
                        );
                        s = Py::new(py, session).unwrap();
                        s = self.tick(s, agent, &t);

                        for r in tick_result {
                            if self.agent_on_update {
                                s = self.update(s, agent, r.update_time, r.clone());
                            }
                            order_history.push(r);
                        }
                    }
                    Err(e) => {
                        log::warn!("err {}", e);
                    }
                }
            }
        });

        return order_history;
    }
}

impl BackTester {
    fn tick(
        &mut self,
        session: Py<DummySession>,
        agent: &PyAny,
        trade: &Trade,
    ) -> Py<DummySession> {
        if self.agent_on_tick {
            let result = agent.call_method1(
                "_on_tick",
                (
                    trade.time,
                    &session,
                    trade.order_side.to_string(),
                    trade.price,
                    trade.size,
                ),
            );
            match result {
                Ok(_oK) => {
                    //
                }
                Err(e) => {
                    log::warn!("Call on_tick Error {:?}", e);
                }
            }
        }
        return session;
    }

    fn clock(&mut self, session: Py<DummySession>, agent: &PyAny, clock: i64) -> Py<DummySession> {
        let result = agent.call_method1("_on_clock", (clock, &session));
        match result {
            Ok(_oK) => {
                //
            }
            Err(e) => {
                log::warn!("Call on_clock Error {:?}", e);
            }
        }

        return session;
    }

    fn update(
        &mut self,
        session: Py<DummySession>,
        agent: &PyAny,
        time: MicroSec,
        r: OrderResult,
    ) -> Py<DummySession> {
        let result = agent.call_method1("_on_update", (time, &session, r));

        match result {
            Ok(_oK) => {
                //
            }
            Err(e) => {
                log::warn!("Call on_clock Error {:?}", e);
            }
        }

        return session;
    }

    fn has_want_event(&self, agent: &PyAny, event_function_name: &str) -> bool {
        if agent.dir().contains(event_function_name).unwrap() {
            return true;
        }

        return false;
    }

    fn clock_interval(&self, agent: &PyAny) -> i64 {
        let interval_sec_py = agent.call_method0("clock_interval").unwrap();
        let interval_sec = interval_sec_py.extract::<i64>().unwrap();

        return interval_sec;
    }
}

#[cfg(test)]
mod back_testr_test {
    use super::*;
    use pyo3::prelude::PyModule;
    use pyo3::*;

    #[test]
    fn test_create() {
        let b = BackTester::new("FTX", "BTC-PERP");
    }

    #[test]
    fn test_run() {
        let b = &mut BackTester::new("FTX", "BTC-PERP");

        Python::with_gil(|py| {
            let agent_class = PyModule::from_code(
                py,
                r#"
class Agent:
    def __init__():
        pass

    def on_tick(session, time, side, price, size):
        print(time, side, price, size)
"#,
                "agent.py",
                "agent",
            )
            .unwrap()
            .getattr("Agent")
            .unwrap();

            let agent = agent_class.call0().unwrap();

            b.run(agent);
        });
    }
}
