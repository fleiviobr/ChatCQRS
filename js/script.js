let ws;

function conectar() {
    const room = document.getElementById('room').value;
    const statusDiv = document.getElementById('status');
    const roomNameSpan = document.getElementById('room-name');
    const inputField = document.getElementById('message-input');
    const sendBtn = document.getElementById('send-btn');

    if (!room) return alert("Digite o nome de uma sala!");

    if (ws) ws.close();

    ws = new WebSocket(`ws://localhost:8080/ws?room=${room}`);

    ws.onopen = () => {
        statusDiv.innerHTML = "Conectado";
        statusDiv.style.color = "#57F287";
        roomNameSpan.innerText = room;
        inputField.disabled = false;
        sendBtn.disabled = false;
        document.getElementById('messages').innerHTML = "";
    };

    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);

        const hora = new Date(data.moment).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

        const msgDiv = document.createElement('div');
        msgDiv.className = 'message';
        msgDiv.innerHTML = `
                    <div class="meta"><strong>${data.message_sent.username}</strong> ${hora}</div>
                    <div class="content">${data.message_sent.content}</div>
                `;

        const messagesContainer = document.getElementById('messages');
        messagesContainer.appendChild(msgDiv);

        messagesContainer.scrollTop = messagesContainer.scrollHeight;
    };

    ws.onclose = () => {
        statusDiv.innerHTML = "Desconectado";
        statusDiv.style.color = "#ed4245";
        inputField.disabled = true;
        sendBtn.disabled = true;
    };
}

async function enviarMensagem(event) {
    event.preventDefault();

    const username = document.getElementById('username').value;
    const room = document.getElementById('room').value;
    const inputField = document.getElementById('message-input');
    const content = inputField.value;

    if (!content.trim() || !username.trim()) return;

    const comando = {
        room_id: room,
        username: username,
        content: content
    };

    try {
        await fetch('http://localhost:8080/send', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(comando)
        });

        inputField.value = '';
    } catch (error) {
        console.error("Erro ao enviar comando:", error);
        alert("Falha ao enviar mensagem. A API está rodando?");
    }
}